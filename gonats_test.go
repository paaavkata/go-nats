package gonats

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestApplyProducerDefaults(t *testing.T) {
	t.Setenv(EnvStreamReplicas, "")
	t.Setenv(EnvEnvironment, "")
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

	if cfg.GroupID != "" {
		t.Errorf("group ID must not be defaulted, got %q", cfg.GroupID)
	}
	if cfg.Concurrency != 1 || cfg.MaxAckPending != 0 {
		t.Errorf("expected serial default (1, server MaxAckPending), got %d/%d", cfg.Concurrency, cfg.MaxAckPending)
	}
	if cfg.MaxRetryBackoff != DefaultConsumerMaxRetryBackoff {
		t.Errorf("expected default max retry backoff %v, got %v", DefaultConsumerMaxRetryBackoff, cfg.MaxRetryBackoff)
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

func TestApplyConsumerDefaultsMaxAckPendingFollowsConcurrency(t *testing.T) {
	cfg := &ConsumerConfig{Concurrency: 8}
	applyConsumerDefaults(cfg)
	if cfg.MaxAckPending != 8 {
		t.Errorf("MaxAckPending = %d, want 8", cfg.MaxAckPending)
	}
}

func TestNewConsumerRequiresGroupID(t *testing.T) {
	_, err := NewConsumer(&ConsumerConfig{Topics: []string{"test-topic"}})
	if err == nil || !strings.Contains(err.Error(), "group ID is required") {
		t.Fatalf("NewConsumer without GroupID: err = %v, want group ID required", err)
	}
}

func TestRetryDelayBoundsAndJitter(t *testing.T) {
	base, limit := 2*time.Second, 60*time.Second
	for n, nominal := range map[uint64]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second, 4: 16 * time.Second, 5: 32 * time.Second, 6: 60 * time.Second, 40: 60 * time.Second} {
		for range 50 {
			d := retryDelay(base, limit, n)
			if d < nominal/2 || d >= nominal*3/2 {
				t.Fatalf("delivery %d: delay %v outside [%v, %v)", n, d, nominal/2, nominal*3/2)
			}
		}
	}
	if a, b := retryDelay(base, limit, 4), retryDelay(base, limit, 4); a == b {
		t.Fatalf("two delays for delivery 4 are identical (%v): no jitter", a)
	}
}

func TestMessageContextCarriesTraceID(t *testing.T) {
	if got := (&Message{TraceID: "r-1"}).Context(context.Background()).Value("trace_id"); got != "r-1" {
		t.Fatalf("trace_id = %v, want r-1", got)
	}
	if got := (&Message{}).Context(context.Background()).Value("trace_id"); got != nil {
		t.Fatalf("trace_id = %v, want none", got)
	}
}

func TestAuthOptionFromEnv(t *testing.T) {
	apply := func(o nats.Option) nats.Options {
		var opts nats.Options
		if o != nil {
			_ = o(&opts)
		}
		return opts
	}
	t.Setenv("NATS_CREDS_FILE", "")
	t.Setenv("NATS_TOKEN", "")
	t.Setenv("NATS_USER", "")
	t.Setenv("NATS_PASSWORD", "")
	if authOption(nil) != nil {
		t.Fatal("expected no auth option with empty env")
	}

	t.Setenv("NATS_USER", "cms-service")
	t.Setenv("NATS_PASSWORD", "pw")
	if o := apply(authOption(nil)); o.User != "cms-service" || o.Password != "pw" {
		t.Fatalf("user/password not applied: %q", o.User)
	}
	if o := apply(authOption(&AuthConfig{})); o.User != "cms-service" {
		t.Fatal("empty AuthConfig should fall back to env")
	}
	if o := apply(authOption(&AuthConfig{Username: "explicit", Password: "x"})); o.User != "explicit" {
		t.Fatal("explicit AuthConfig must win over env")
	}

	t.Setenv("NATS_TOKEN", "tok")
	if o := apply(authOption(nil)); o.Token != "tok" || o.User != "" {
		t.Fatal("token should win over user/password")
	}
}
