package gonats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/viper"
)

const (
	DefaultProducerTimeout = 5 * time.Second
)

// Producer publishes JSON messages to a single topic (JetStream stream),
// mirroring go-kafka's Producer API. Publishes are synchronous: they return
// after the server acknowledges persistence (Kafka acks=all equivalent).
type Producer struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	topic  string
	scoped bool // stream captures "<topic>.*"; SendScoped* allowed
	config *ProducerConfig
}

// ProducerConfig holds the configuration for producers.
type ProducerConfig struct {
	URLs     []string      // NATS server URLs (e.g. nats://nats.data-dev.svc:4222)
	ClientID string        // connection name, shows up in NATS monitoring
	Topic    string        // base topic == stream name (never "<base>.<app>")
	Timeout  time.Duration // per-publish ack timeout
	Stream   StreamConfig  // used only when the stream does not exist yet
	TLS      *TLSConfig
	Auth     *AuthConfig
	// AppScoped makes the stream capture "<Topic>.*" and enables SendScoped /
	// SendScopedWithMsgID. Implied for AppScopedTopics, so it only needs
	// setting for a base outside that list.
	AppScoped bool
}

// NewProducerConfigFromViper creates a producer configuration from Viper.
//
// Keys: nats.urls, nats.client_id, nats.topic, nats.producer.timeout,
// nats.stream.{max_age,max_bytes,replicas}, nats.tls.*, nats.auth.*
func NewProducerConfigFromViper() *ProducerConfig {
	config := &ProducerConfig{
		URLs:     viper.GetStringSlice("nats.urls"),
		ClientID: viper.GetString("nats.client_id"),
		Topic:    viper.GetString("nats.topic"),
		Timeout:  viper.GetDuration("nats.producer.timeout"),
		Stream:   streamConfigFromViper(),
		TLS:      tlsConfigFromViper(),
		Auth:     authConfigFromViper(),
	}
	applyProducerDefaults(config)
	return config
}

// applyProducerDefaults fills zero-value fields so partial structs are valid.
// Safe to call multiple times.
func applyProducerDefaults(config *ProducerConfig) {
	if len(config.URLs) == 0 {
		config.URLs = []string{DefaultURL}
	}
	if config.ClientID == "" {
		config.ClientID = "file-convert-producer"
	}
	if config.Timeout == 0 {
		config.Timeout = DefaultProducerTimeout
	}
	applyStreamDefaults(&config.Stream)
}

// NewProducer creates a new producer bound to config.Topic. The backing
// stream is created if it does not exist.
func NewProducer(config *ProducerConfig) (*Producer, error) {
	if config == nil {
		config = NewProducerConfigFromViper()
	} else {
		applyProducerDefaults(config)
	}

	if config.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	nc, err := connect(config.URLs, config.ClientID, config.TLS, config.Auth)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	scoped := config.AppScoped || IsAppScopedTopic(config.Topic)
	if scoped {
		if err := validateBaseTopic(config.Topic); err != nil {
			nc.Close()
			return nil, err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()
	if err := ensureStream(ctx, js, config.Topic, config.Stream, scoped); err != nil {
		nc.Close()
		return nil, err
	}

	return &Producer{
		nc:     nc,
		js:     js,
		topic:  config.Topic,
		scoped: scoped,
		config: config,
	}, nil
}

// Topic returns the base topic (stream) this producer is bound to.
func (p *Producer) Topic() string {
	return p.topic
}

// Close drains pending publishes and closes the connection.
func (p *Producer) Close() error {
	if p.nc != nil && !p.nc.IsClosed() {
		p.nc.Close()
	}
	return nil
}

// SendMessage marshals value to JSON and publishes it, waiting for the
// JetStream ack. key is carried in the Nats-Msg-Key header (may be empty).
func (p *Producer) SendMessage(key string, value interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.config.Timeout)
	defer cancel()
	return p.SendMessageWithContext(ctx, key, value)
}

// SendMessageWithContext is SendMessage with caller-controlled cancellation.
// It always publishes on the flat (legacy) subject == Topic.
func (p *Producer) SendMessageWithContext(ctx context.Context, key string, value interface{}) error {
	return p.publish(ctx, p.topic, key, "", value)
}

// SendScoped publishes value on the app-scoped subject "<Topic>.<appID>" and
// waits for the JetStream ack. key is carried in the Nats-Msg-Key header (may
// be empty). The producer must be app-scoped (ProducerConfig.AppScoped or an
// AppScopedTopics base); appID must pass ValidateAppID.
func (p *Producer) SendScoped(ctx context.Context, appID, key string, value interface{}) error {
	return p.SendScopedWithMsgID(ctx, appID, key, "", value)
}

// SendScopedWithMsgID is SendScoped plus a Nats-Msg-Id header: a repeat
// publish with the same non-empty msgID inside the stream's duplicate window
// (2m default) is acknowledged but stored only once, which makes publish
// retries idempotent. Use a stable producer-side ID (event UID). An empty
// msgID sends no header (same as SendScoped).
func (p *Producer) SendScopedWithMsgID(ctx context.Context, appID, key, msgID string, value interface{}) error {
	if !p.scoped {
		return fmt.Errorf("producer for topic %q is not app-scoped (set ProducerConfig.AppScoped)", p.topic)
	}
	subject, err := ScopedSubject(p.topic, appID)
	if err != nil {
		return err
	}
	return p.publish(ctx, subject, key, msgID, value)
}

// publish marshals value and publishes it on subject, waiting for the ack.
func (p *Producer) publish(ctx context.Context, subject, key, msgID string, value interface{}) error {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	msg := &nats.Msg{
		Subject: subject,
		Data:    jsonValue,
		Header:  nats.Header{},
	}
	if key != "" {
		msg.Header.Set(KeyHeader, key)
	}
	if msgID != "" {
		msg.Header.Set(MsgIDHeader, msgID)
	}

	if _, err := p.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("failed to send message to %q: %w", subject, err)
	}
	return nil
}
