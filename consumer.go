package gonats

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
)

const (
	DefaultConsumerGroupID      = "file-convert-group"
	DefaultConsumerAckWait      = 30 * time.Second
	DefaultConsumerMaxDeliver   = 5
	DefaultConsumerRetryBackoff = 2 * time.Second
	// maxIteratorBackoff caps the wait between attempts to rebuild a failed
	// message iterator (see consumeTopic).
	maxIteratorBackoff = 30 * time.Second
)

// Consumer consumes messages from one or more topics as part of a "group"
// (durable JetStream consumer), mirroring go-kafka's Consumer API.
//
// Delivery semantics: at-least-once. When the handler returns nil the message
// is acked; on error it is redelivered after RetryBackoff, up to MaxDeliver
// attempts. On the last failed attempt the message is copied to the
// dead-letter stream (see deadletter.go) and terminated, so nothing is dropped
// silently. A handler panic is recovered and treated as a handler error.
// Messages within a topic are processed serially, preserving stream order
// (stronger than Kafka's per-partition ordering).
type Consumer struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	config *ConsumerConfig
	subs   []subscription

	mu      sync.Mutex
	loopErr error // last unexpected exit of ConsumeMessages
	closed  bool

	deadLetters  *prometheus.CounterVec
	handlerPanic *prometheus.CounterVec
}

// TopicSubscription selects which subjects of one base topic a consumer reads.
// Build it with SubscribeApp or SubscribeAllApps; the zero mode (neither AppID
// nor AllApps) is the legacy flat-subject subscription, identical to listing
// the topic in ConsumerConfig.Topics.
type TopicSubscription struct {
	Topic   string // base topic == stream (never "<base>.<app>")
	AppID   string // non-empty: only "<Topic>.<AppID>"
	AllApps bool   // "<Topic>" and every "<Topic>.<app>"
}

// SubscribeApp consumes only app appID's messages on base: one durable
// "<group>_<appID>" filtered on "<base>.<appID>". Legacy flat-subject
// messages are NOT delivered to it.
func SubscribeApp(base, appID string) TopicSubscription {
	return TopicSubscription{Topic: base, AppID: appID}
}

// SubscribeAllApps consumes every app's messages on base plus legacy
// flat-subject ones, through TWO durables on the same stream:
//
//	"<group>"       filter "<base>"    the legacy durable, reused untouched
//	"<group>__all"  filter "<base>.*"  new; DeliverPolicy from AutoOffsetReset
//
// Why not one durable with two filter subjects: widening the legacy durable
// in place does not rewind it, so "<base>.<app>" messages it already scanned
// past (published before the widening) are silently skipped; and a NEW
// durable over both subjects would replay the retained legacy backlog. Two
// durables have neither problem: the legacy one keeps its ack floor, and the
// scoped one (with the default "earliest") picks up every retained scoped
// message regardless of whether producers or consumers deploy first.
// Ordering between legacy and scoped messages is not preserved (they are
// consumed concurrently, like two topics).
func SubscribeAllApps(base string) TopicSubscription {
	return TopicSubscription{Topic: base, AllApps: true}
}

// subscription is one resolved durable consumer on one stream.
type subscription struct {
	topic   string // base topic == stream subject base
	stream  string // stream name
	durable string // durable consumer name
	filter  string // consumer filter subject
	scoped  bool   // stream must capture "<topic>.*"
}

// resolveSubscriptions turns Topics (legacy) and Subscriptions into the
// concrete per-stream durables, rejecting invalid or colliding entries.
func resolveSubscriptions(config *ConsumerConfig) ([]subscription, error) {
	all := make([]TopicSubscription, 0, len(config.Topics)+len(config.Subscriptions))
	for _, t := range config.Topics {
		all = append(all, TopicSubscription{Topic: t})
	}
	all = append(all, config.Subscriptions...)

	var subs []subscription
	seen := make(map[string]bool, len(all))
	add := func(sub subscription) error {
		key := sub.stream + "/" + sub.durable
		if seen[key] {
			return fmt.Errorf("duplicate subscription: durable %q on stream %q (topic %q listed twice, or legacy and all-apps on the same topic)",
				sub.durable, sub.stream, sub.topic)
		}
		seen[key] = true
		subs = append(subs, sub)
		return nil
	}

	for _, ts := range all {
		if ts.Topic == "" {
			return nil, fmt.Errorf("subscription topic is required")
		}
		base := subscription{topic: ts.Topic, stream: StreamName(ts.Topic)}
		var resolved []subscription
		switch {
		case ts.AppID != "" && ts.AllApps:
			return nil, fmt.Errorf("subscription on %q: AppID and AllApps are mutually exclusive", ts.Topic)
		case ts.AppID != "":
			subject, err := ScopedSubject(ts.Topic, ts.AppID)
			if err != nil {
				return nil, err
			}
			app := base
			app.durable, app.filter, app.scoped = DurableName(config.GroupID, ts.AppID), subject, true
			resolved = []subscription{app}
		case ts.AllApps:
			if err := validateBaseTopic(ts.Topic); err != nil {
				return nil, err
			}
			legacy := base
			legacy.durable, legacy.filter, legacy.scoped = DurableName(config.GroupID, ""), ts.Topic, true
			scoped := base
			scoped.durable, scoped.filter, scoped.scoped = AllAppsDurableName(config.GroupID), ts.Topic+".*", true
			resolved = []subscription{legacy, scoped}
		default:
			legacy := base
			legacy.durable, legacy.filter = DurableName(config.GroupID, ""), ts.Topic
			resolved = []subscription{legacy}
		}
		for _, sub := range resolved {
			if err := add(sub); err != nil {
				return nil, err
			}
		}
	}
	return subs, nil
}

// ConsumerConfig holds the configuration for consumers.
type ConsumerConfig struct {
	URLs             []string
	GroupID          string   // durable consumer name (one per consuming service)
	Topics           []string // legacy flat-subject topics; each topic is its own stream
	AutoOffsetReset  string   // "earliest" (default) or "latest" — applies on first creation only
	MaxDeliver       int      // max delivery attempts before the server stops redelivering
	AckWait          time.Duration
	RetryBackoff     time.Duration // Nak delay after a handler error
	EnableAutoCommit bool          // kept for go-kafka API compat; acks are always explicit
	Stream           StreamConfig  // used only when a stream does not exist yet
	TLS              *TLSConfig
	Auth             *AuthConfig
	// InactiveThreshold instructs the NATS server to auto-delete a durable
	// consumer after it has had no active subscription for this duration.
	// Zero (default) preserves current behaviour: the server never
	// auto-deletes the consumer.  Set to e.g. 24h for per-pod durables so
	// that consumers left behind by pod restarts/deploys are cleaned up
	// automatically.  This field is wired on first consumer creation only;
	// existing durables are not updated (JetStream reuse path, line ~189).
	InactiveThreshold time.Duration
	// Subscriptions adds app-scoped subscriptions (SubscribeApp /
	// SubscribeAllApps); may be combined with Topics for other streams.
	Subscriptions []TopicSubscription
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

	if len(config.Topics) == 0 && len(config.Subscriptions) == 0 {
		return nil, fmt.Errorf("at least one topic is required")
	}
	subs, err := resolveSubscriptions(config)
	if err != nil {
		return nil, fmt.Errorf("invalid consumer config: %w", err)
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
		subs:   subs,
	}, nil
}

// Close closes the underlying connection. Pending unacked messages are
// redelivered to the group after AckWait.
func (c *Consumer) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	if c.nc != nil && !c.nc.IsClosed() {
		c.nc.Close()
	}
	return nil
}

// Healthy reports whether the consume loop is alive. It returns the error that
// made ConsumeMessages exit unexpectedly (iterator failure, consumer deleted
// server-side, ...), nil while the loop runs or after a clean Close. Wire it
// into readiness: mgr.AddReadinessCheck("nats-consumer", consumer.Healthy).
func (c *Consumer) Healthy(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	return c.loopErr
}

func (c *Consumer) setLoopErr(err error) {
	c.mu.Lock()
	c.loopErr = err
	c.mu.Unlock()
}

// RegisterMetrics registers the consumer's counters on reg:
//
//	gonats_dead_letter_total{topic}     messages moved to the dead-letter stream
//	gonats_handler_panics_total{topic}  handler panics recovered
//
// Call once per process (a second registration on the same registry errors).
func (c *Consumer) RegisterMetrics(reg prometheus.Registerer) error {
	dl := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gonats_dead_letter_total",
		Help: "Messages copied to the dead-letter stream after exhausting MaxDeliver.",
	}, []string{"topic"})
	hp := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gonats_handler_panics_total",
		Help: "Handler panics recovered by the consumer loop.",
	}, []string{"topic"})
	if err := reg.Register(dl); err != nil {
		return err
	}
	if err := reg.Register(hp); err != nil {
		return err
	}
	c.mu.Lock()
	c.deadLetters, c.handlerPanic = dl, hp
	c.mu.Unlock()
	return nil
}

func (c *Consumer) countDeadLetter(topic string) {
	if c.deadLetters != nil {
		c.deadLetters.WithLabelValues(topic).Inc()
	}
}

func (c *Consumer) countPanic(topic string) {
	if c.handlerPanic != nil {
		c.handlerPanic.WithLabelValues(topic).Inc()
	}
}

// ConsumeMessages consumes from all configured topics and subscriptions until ctx is cancelled
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
	c.setLoopErr(nil)

	// Best effort: a missing dead-letter stream must not stop consumption;
	// deadLetter() logs the publish failure per message instead.
	if err := EnsureDeadLetterStream(runCtx, c.js, c.config.Stream); err != nil {
		logPrintf("dead-letter stream unavailable: %v", err)
	}

	for _, sub := range c.subs {
		cons, err := c.ensureConsumer(runCtx, sub)
		if err != nil {
			cancel()
			wg.Wait()
			c.setLoopErr(err)
			return err
		}

		wg.Add(1)
		go func(sub subscription, cons jetstream.Consumer) {
			defer wg.Done()
			if err := c.consumeTopic(runCtx, sub, cons, handler); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				cancel() // one topic failing hard stops the whole consumer
			}
		}(sub, cons)
	}

	wg.Wait()
	if ctx.Err() != nil {
		return nil // context cancellation is a clean shutdown, not an error
	}
	if firstErr != nil {
		c.setLoopErr(firstErr)
	}
	return firstErr
}

// ensureConsumer makes sure the stream exists and returns the durable
// consumer for sub, creating it on first use.
func (c *Consumer) ensureConsumer(ctx context.Context, sub subscription) (jetstream.Consumer, error) {
	if err := ensureStream(ctx, c.js, sub.topic, c.config.Stream, sub.scoped); err != nil {
		return nil, err
	}

	// Reuse the existing durable if present — avoids update conflicts on
	// immutable fields like DeliverPolicy.
	cons, err := c.js.Consumer(ctx, sub.stream, sub.durable)
	if err == nil {
		return cons, nil
	}
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil, fmt.Errorf("failed to look up consumer %q on stream %q: %w", sub.durable, sub.stream, err)
	}

	deliverPolicy := jetstream.DeliverAllPolicy
	if c.config.AutoOffsetReset == "latest" {
		deliverPolicy = jetstream.DeliverNewPolicy
	}

	cons, err = c.js.CreateConsumer(ctx, sub.stream, jetstream.ConsumerConfig{
		Durable:           sub.durable,
		FilterSubject:     sub.filter,
		DeliverPolicy:     deliverPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           c.config.AckWait,
		MaxDeliver:        c.config.MaxDeliver,
		InactiveThreshold: c.config.InactiveThreshold,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer %q on stream %q: %w", sub.durable, sub.stream, err)
	}
	return cons, nil
}

// consumeTopic keeps one topic consuming until ctx is cancelled. A failed
// iterator (JetStream quorum loss, "no heartbeat received", consumer deleted
// server-side, reconnect) is rebuilt after a capped exponential backoff
// instead of ending the consumer for the life of the pod. Healthy reports the
// fetch error while recovery is in progress and clears once messages flow.
func (c *Consumer) consumeTopic(ctx context.Context, sub subscription, cons jetstream.Consumer, handler func(*Message) error) error {
	backoff := c.config.RetryBackoff
	for {
		progressed, err := c.consumeIterator(ctx, sub, cons, handler)
		if err == nil || ctx.Err() != nil {
			return nil
		}
		if progressed {
			backoff = c.config.RetryBackoff
		}
		c.setLoopErr(err)
		logPrintf("consumer topic=%q fetch failed, retrying in %v: %v", sub.topic, backoff, err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < maxIteratorBackoff {
			backoff *= 2
		}
		// A deleted or missing durable has to be recreated; other errors reuse
		// the handle (Messages() opens a fresh pull subscription each time).
		if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) {
			if cons, err = c.ensureConsumer(ctx, sub); err != nil {
				logPrintf("consumer topic=%q recreate failed: %v", sub.topic, err)
				cons = nil
			}
		}
		if cons == nil {
			if cons, err = c.ensureConsumer(ctx, sub); err != nil {
				cons = nil
				continue
			}
		}
	}
}

// consumeIterator runs the serial fetch/handle/ack loop for one topic over a
// single message iterator. progressed reports whether at least one message
// was fetched before the iterator failed.
func (c *Consumer) consumeIterator(ctx context.Context, sub subscription, cons jetstream.Consumer, handler func(*Message) error) (progressed bool, retErr error) {
	topic := sub.topic
	it, err := cons.Messages()
	if err != nil {
		return false, fmt.Errorf("failed to start message iterator for topic %q: %w", topic, err)
	}
	// A rebuilt iterator on an idle topic never fetches a message, so clearing
	// loopErr only on progress left pods unready for hours after one missed
	// heartbeat (2026-11-02). Messages() succeeds even while disconnected
	// (nats.go buffers the subscribe), so prove the server is reachable and
	// the durable exists with an Info round-trip before reporting healthy.
	if c.Healthy(ctx) != nil {
		infoCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, infoErr := cons.Info(infoCtx)
		cancel()
		if infoErr == nil {
			c.setLoopErr(nil) // recovered
		}
	}

	// Unblock it.Next() when the context is cancelled. `done` lets the watcher
	// exit when the loop returns on its own (iterator error): before this, the
	// deferred <-stopped waited for a ctx cancel that ConsumeMessages only
	// issues after consumeTopic returns, so a dead iterator hung the topic
	// goroutine forever and the pod stayed Ready with no consumer.
	stopped := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
		case <-done:
		}
		it.Stop()
	}()
	// Deferred LIFO: close(done) runs first, then wait for the watcher.
	defer func() { <-stopped }()
	defer close(done)

	for {
		msg, err := it.Next()
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgIteratorClosed) || ctx.Err() != nil {
				return progressed, nil
			}
			return progressed, fmt.Errorf("failed to fetch message from topic %q: %w", topic, err)
		}
		progressed = true
		if c.Healthy(ctx) != nil {
			c.setLoopErr(nil) // recovered
		}

		if err := c.safeHandle(topic, handler, toMessage(topic, msg)); err != nil {
			if c.lastDelivery(msg) {
				// Last attempt: park a copy on the dead-letter stream, then
				// terminate so the server stops redelivering. If the DLQ
				// publish fails, leave the message to the server (it is
				// dropped after MaxDeliver either way) and log loudly.
				if dlqErr := c.deadLetter(ctx, sub, msg, err); dlqErr != nil {
					logPrintf("dead-letter publish failed topic=%q subject=%q msg_id=%q: %v (handler error: %v)",
						topic, msg.Subject(), msg.Headers().Get(MsgIDHeader), dlqErr, err)
					_ = msg.NakWithDelay(c.config.RetryBackoff)
					continue
				}
				c.countDeadLetter(topic)
				logPrintf("dead-lettered topic=%q subject=%q after %d deliveries: %v", topic, msg.Subject(), c.config.MaxDeliver, err)
				_ = msg.Term()
				continue
			}
			// Redeliver after backoff.
			_ = msg.NakWithDelay(c.config.RetryBackoff)
			continue
		}
		_ = msg.Ack()
	}
}

// safeHandle invokes handler, converting a panic into an error so one poison
// message cannot crash the process.
func (c *Consumer) safeHandle(topic string, handler func(*Message) error, m *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.countPanic(topic)
			logPrintf("handler panic topic=%q subject=%q: %v\n%s", topic, m.Subject, r, debug.Stack())
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return handler(m)
}

// lastDelivery reports whether msg is on its final delivery attempt.
// MaxDeliver <= 0 means unlimited redelivery, so never true.
func (c *Consumer) lastDelivery(msg jetstream.Msg) bool {
	if c.config.MaxDeliver <= 0 {
		return false
	}
	meta, err := msg.Metadata()
	if err != nil {
		return false
	}
	return meta.NumDelivered >= uint64(c.config.MaxDeliver)
}

// toMessage converts a JetStream message into the shared Message type. topic
// is the base topic; the concrete subject and its app scope are carried too.
func toMessage(topic string, msg jetstream.Msg) *Message {
	m := &Message{
		Topic:   topic,
		Key:     []byte(msg.Headers().Get(KeyHeader)),
		Value:   msg.Data(),
		Subject: msg.Subject(),
		AppID:   AppIDFromSubject(topic, msg.Subject()),
	}
	if meta, err := msg.Metadata(); err == nil {
		m.Offset = int64(meta.Sequence.Stream)
		m.Timestamp = meta.Timestamp
	}
	return m
}
