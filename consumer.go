package gonats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/viper"
)

const (
	DefaultConsumerGroupID      = "file-convert-group"
	DefaultConsumerAckWait      = 30 * time.Second
	DefaultConsumerMaxDeliver   = 5
	DefaultConsumerRetryBackoff = 2 * time.Second
)

// Consumer consumes messages from one or more topics as part of a "group"
// (durable JetStream consumer), mirroring go-kafka's Consumer API.
//
// Delivery semantics: at-least-once. When the handler returns nil the message
// is acked; on error it is redelivered after RetryBackoff, up to MaxDeliver
// attempts. Messages within a topic are processed serially, preserving stream
// order (stronger than Kafka's per-partition ordering).
type Consumer struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	config *ConsumerConfig
}

// ConsumerConfig holds the configuration for consumers.
type ConsumerConfig struct {
	URLs              []string
	GroupID           string   // durable consumer name (one per consuming service)
	Topics            []string // each topic is its own stream
	AutoOffsetReset   string   // "earliest" (default) or "latest" — applies on first creation only
	MaxDeliver        int      // max delivery attempts before the server stops redelivering
	AckWait           time.Duration
	RetryBackoff      time.Duration // Nak delay after a handler error
	EnableAutoCommit  bool          // kept for go-kafka API compat; acks are always explicit
	Stream            StreamConfig  // used only when a stream does not exist yet
	TLS               *TLSConfig
	Auth              *AuthConfig
	// InactiveThreshold instructs the NATS server to auto-delete a durable
	// consumer after it has had no active subscription for this duration.
	// Zero (default) preserves current behaviour: the server never
	// auto-deletes the consumer.  Set to e.g. 24h for per-pod durables so
	// that consumers left behind by pod restarts/deploys are cleaned up
	// automatically.  This field is wired on first consumer creation only;
	// existing durables are not updated (JetStream reuse path, line ~189).
	InactiveThreshold time.Duration
}

// NewConsumerConfigFromViper creates a consumer configuration from Viper.
//
// Keys: nats.urls, nats.consumer.{group_id,topics,auto_offset_reset,
// max_deliver,ack_wait,retry_backoff,inactive_threshold}, nats.stream.*,
// nats.tls.*, nats.auth.*
func NewConsumerConfigFromViper() *ConsumerConfig {
	config := &ConsumerConfig{
		URLs:              viper.GetStringSlice("nats.urls"),
		GroupID:           viper.GetString("nats.consumer.group_id"),
		Topics:            viper.GetStringSlice("nats.consumer.topics"),
		AutoOffsetReset:   viper.GetString("nats.consumer.auto_offset_reset"),
		MaxDeliver:        viper.GetInt("nats.consumer.max_deliver"),
		AckWait:           viper.GetDuration("nats.consumer.ack_wait"),
		RetryBackoff:      viper.GetDuration("nats.consumer.retry_backoff"),
		InactiveThreshold: viper.GetDuration("nats.consumer.inactive_threshold"),
		Stream:            streamConfigFromViper(),
		TLS:               tlsConfigFromViper(),
		Auth:              authConfigFromViper(),
	}
	applyConsumerDefaults(config)
	return config
}

// applyConsumerDefaults fills zero-value fields so partial structs are valid.
// Safe to call multiple times.
func applyConsumerDefaults(config *ConsumerConfig) {
	if len(config.URLs) == 0 {
		config.URLs = []string{DefaultURL}
	}
	if config.GroupID == "" {
		config.GroupID = DefaultConsumerGroupID
	}
	if config.AutoOffsetReset == "" {
		config.AutoOffsetReset = "earliest"
	}
	if config.MaxDeliver == 0 {
		config.MaxDeliver = DefaultConsumerMaxDeliver
	}
	if config.AckWait == 0 {
		config.AckWait = DefaultConsumerAckWait
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = DefaultConsumerRetryBackoff
	}
	applyStreamDefaults(&config.Stream)
}

// NewConsumer creates a new consumer. Streams for all configured topics are
// created if they do not exist, so consumers may start before producers.
func NewConsumer(config *ConsumerConfig) (*Consumer, error) {
	if config == nil {
		config = NewConsumerConfigFromViper()
	} else {
		applyConsumerDefaults(config)
	}

	if len(config.Topics) == 0 {
		return nil, fmt.Errorf("at least one topic is required")
	}

	nc, err := connect(config.URLs, config.GroupID, config.TLS, config.Auth)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &Consumer{
		nc:     nc,
		js:     js,
		config: config,
	}, nil
}

// Close closes the underlying connection. Pending unacked messages are
// redelivered to the group after AckWait.
func (c *Consumer) Close() error {
	if c.nc != nil && !c.nc.IsClosed() {
		c.nc.Close()
	}
	return nil
}

// ConsumeMessages consumes from all configured topics until ctx is cancelled
// (clean shutdown, returns nil) or a non-recoverable error occurs. The handler
// may be invoked concurrently for different topics, but serially within one.
func (c *Consumer) ConsumeMessages(ctx context.Context, handler func(*Message) error) error {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, topic := range c.config.Topics {
		cons, err := c.ensureConsumer(runCtx, topic)
		if err != nil {
			cancel()
			wg.Wait()
			return err
		}

		wg.Add(1)
		go func(topic string, cons jetstream.Consumer) {
			defer wg.Done()
			if err := c.consumeTopic(runCtx, topic, cons, handler); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				cancel() // one topic failing hard stops the whole consumer
			}
		}(topic, cons)
	}

	wg.Wait()
	if ctx.Err() != nil {
		return nil // context cancellation is a clean shutdown, not an error
	}
	return firstErr
}

// ensureConsumer makes sure the stream exists and returns the group's durable
// consumer on it, creating the consumer on first use.
func (c *Consumer) ensureConsumer(ctx context.Context, topic string) (jetstream.Consumer, error) {
	if err := ensureStream(ctx, c.js, topic, c.config.Stream); err != nil {
		return nil, err
	}

	streamName := sanitizeName(topic)
	durable := sanitizeName(c.config.GroupID)

	// Reuse the existing durable if present — avoids update conflicts on
	// immutable fields like DeliverPolicy.
	cons, err := c.js.Consumer(ctx, streamName, durable)
	if err == nil {
		return cons, nil
	}
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil, fmt.Errorf("failed to look up consumer %q on stream %q: %w", durable, streamName, err)
	}

	deliverPolicy := jetstream.DeliverAllPolicy
	if c.config.AutoOffsetReset == "latest" {
		deliverPolicy = jetstream.DeliverNewPolicy
	}

	cons, err = c.js.CreateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:           durable,
		FilterSubject:     topic,
		DeliverPolicy:     deliverPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           c.config.AckWait,
		MaxDeliver:        c.config.MaxDeliver,
		InactiveThreshold: c.config.InactiveThreshold,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer %q on stream %q: %w", durable, streamName, err)
	}
	return cons, nil
}

// consumeTopic runs the serial fetch/handle/ack loop for one topic.
func (c *Consumer) consumeTopic(ctx context.Context, topic string, cons jetstream.Consumer, handler func(*Message) error) error {
	it, err := cons.Messages()
	if err != nil {
		return fmt.Errorf("failed to start message iterator for topic %q: %w", topic, err)
	}

	// Unblock it.Next() when the context is cancelled.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		it.Stop()
	}()
	defer func() { <-stopped }()

	for {
		msg, err := it.Next()
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgIteratorClosed) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("failed to fetch message from topic %q: %w", topic, err)
		}

		if err := handler(toMessage(topic, msg)); err != nil {
			// Redeliver after backoff; the server gives up after MaxDeliver.
			_ = msg.NakWithDelay(c.config.RetryBackoff)
			continue
		}
		_ = msg.Ack()
	}
}

// toMessage converts a JetStream message into the shared Message type.
func toMessage(topic string, msg jetstream.Msg) *Message {
	m := &Message{
		Topic: topic,
		Key:   []byte(msg.Headers().Get(KeyHeader)),
		Value: msg.Data(),
	}
	if meta, err := msg.Metadata(); err == nil {
		m.Offset = int64(meta.Sequence.Stream)
		m.Timestamp = meta.Timestamp
	}
	return m
}
