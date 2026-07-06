package gonats

import (
	"context"
	"encoding/json"
	"errors"
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
	config *ProducerConfig
}

// ProducerConfig holds the configuration for producers.
type ProducerConfig struct {
	URLs     []string // NATS server URLs (e.g. nats://nats.data-dev.svc:4222)
	ClientID string   // connection name, shows up in NATS monitoring
	Topic    string
	Timeout  time.Duration // per-publish ack timeout
	Stream   StreamConfig  // used only when the stream does not exist yet
	TLS      *TLSConfig
	Auth     *AuthConfig
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

	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()
	if err := ensureStream(ctx, js, config.Topic, config.Stream); err != nil {
		nc.Close()
		return nil, err
	}

	return &Producer{
		nc:     nc,
		js:     js,
		topic:  config.Topic,
		config: config,
	}, nil
}

// ensureStream idempotently creates the stream backing a topic. An existing
// stream is left untouched so externally managed configuration wins.
func ensureStream(ctx context.Context, js jetstream.JetStream, topic string, cfg StreamConfig) error {
	applyStreamDefaults(&cfg)
	name := sanitizeName(topic)
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{topic},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardOld,
		MaxAge:    cfg.MaxAge,
		MaxBytes:  cfg.MaxBytes,
		Replicas:  cfg.Replicas,
	})
	if err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return fmt.Errorf("failed to ensure stream %q: %w", name, err)
	}
	return nil
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
func (p *Producer) SendMessageWithContext(ctx context.Context, key string, value interface{}) error {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	msg := &nats.Msg{
		Subject: p.topic,
		Data:    jsonValue,
		Header:  nats.Header{},
	}
	if key != "" {
		msg.Header.Set(KeyHeader, key)
	}

	if _, err := p.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	return nil
}
