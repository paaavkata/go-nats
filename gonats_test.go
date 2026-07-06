package gonats

import (
	"testing"
	"time"
)

func TestApplyProducerDefaults(t *testing.T) {
	cfg := &ProducerConfig{Topic: "test-topic"}
	applyProducerDefaults(cfg)

	if len(cfg.URLs) != 1 || cfg.URLs[0] != DefaultURL {
		t.Errorf("expected default URL %q, got %v", DefaultURL, cfg.URLs)
	}
	if cfg.ClientID == "" {
		t.Error("expected default client ID")
	}
	if cfg.Timeout != DefaultProducerTimeout {
		t.Errorf("expected default timeout %v, got %v", DefaultProducerTimeout, cfg.Timeout)
	}
	if cfg.Stream.MaxAge != DefaultStreamMaxAge {
		t.Errorf("expected default stream max age %v, got %v", DefaultStreamMaxAge, cfg.Stream.MaxAge)
	}
	if cfg.Stream.Replicas != DefaultStreamReplicas {
		t.Errorf("expected default stream replicas %d, got %d", DefaultStreamReplicas, cfg.Stream.Replicas)
	}
}

func TestApplyProducerDefaultsKeepsExplicitValues(t *testing.T) {
	cfg := &ProducerConfig{
		URLs:    []string{"nats://nats.data-dev.svc:4222"},
		Topic:   "test-topic",
		Timeout: 10 * time.Second,
		Stream:  StreamConfig{MaxAge: time.Hour, Replicas: 3},
	}
	applyProducerDefaults(cfg)

	if cfg.URLs[0] != "nats://nats.data-dev.svc:4222" {
		t.Errorf("explicit URL overwritten: %v", cfg.URLs)
	}
	if cfg.Timeout != 10*time.Second {
		t.Errorf("explicit timeout overwritten: %v", cfg.Timeout)
	}
	if cfg.Stream.MaxAge != time.Hour || cfg.Stream.Replicas != 3 {
		t.Errorf("explicit stream config overwritten: %+v", cfg.Stream)
	}
}

func TestApplyConsumerDefaults(t *testing.T) {
	cfg := &ConsumerConfig{Topics: []string{"test-topic"}}
	applyConsumerDefaults(cfg)

	if cfg.GroupID != DefaultConsumerGroupID {
		t.Errorf("expected default group ID %q, got %q", DefaultConsumerGroupID, cfg.GroupID)
	}
	if cfg.AutoOffsetReset != "earliest" {
		t.Errorf("expected default auto offset reset %q, got %q", "earliest", cfg.AutoOffsetReset)
	}
	if cfg.MaxDeliver != DefaultConsumerMaxDeliver {
		t.Errorf("expected default max deliver %d, got %d", DefaultConsumerMaxDeliver, cfg.MaxDeliver)
	}
	if cfg.AckWait != DefaultConsumerAckWait {
		t.Errorf("expected default ack wait %v, got %v", DefaultConsumerAckWait, cfg.AckWait)
	}
	if cfg.RetryBackoff != DefaultConsumerRetryBackoff {
		t.Errorf("expected default retry backoff %v, got %v", DefaultConsumerRetryBackoff, cfg.RetryBackoff)
	}
}

func TestSanitizeName(t *testing.T) {
	cases := map[string]string{
		"payment-events":       "payment-events",
		"my.group.id":          "my-group-id",
		"weird topic*name>":    "weird-topic-name-",
		"conversion-status-v2": "conversion-status-v2",
	}
	for in, want := range cases {
		if got := sanitizeName(in); got != want {
			t.Errorf("sanitizeName(%q) = %q, want %q", in, got, want)
		}
	}
}
