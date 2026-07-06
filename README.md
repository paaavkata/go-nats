# Go NATS Library

Shared event-bus library for FileConvert microservices, backed by **NATS
JetStream**. Drop-in successor to [`go-kafka`](../go-kafka) — same API shape
(`Producer` / `Consumer` / `Message`, Viper constructors), so migration is
mostly an import + config-key swap.

## Concept mapping (Kafka → NATS)

| Kafka                      | NATS JetStream                                        |
|----------------------------|-------------------------------------------------------|
| Topic                      | Stream (name == topic) with one subject (== topic)    |
| Message key                | `Nats-Msg-Key` header (streams are totally ordered)   |
| Consumer group             | Durable pull consumer named after the group ID        |
| `auto.offset.reset`        | `DeliverPolicy` (`earliest` = all, `latest` = new)    |
| `retention.ms`             | Stream `MaxAge`                                       |
| Partition / offset         | Always `0` / stream sequence (in `Message`)           |

Streams are created **idempotently** by both producers and consumers, so
service startup order doesn't matter. Existing streams are never modified —
externally managed configuration wins.

## Delivery semantics

At-least-once. Handler returns `nil` → ack. Handler returns error → redelivered
after `RetryBackoff`, up to `MaxDeliver` attempts, then the server stops
redelivering. Messages within one topic are processed serially (preserves
order); different topics are consumed concurrently.

## Configuration (Viper keys)

### Connection (shared)
- `nats.urls`: NATS server URLs (default: `["nats://localhost:4222"]`)
- `nats.client_id`: connection name shown in NATS monitoring
- `nats.tls.enabled|ca_file|cert_file|key_file|insecure_skip_verify`
- `nats.auth.username|password|token|creds_file`

### Stream creation (used only when a stream doesn't exist yet)
- `nats.stream.max_age`: retention window (default: `168h`)
- `nats.stream.max_bytes`: size cap per stream (default: 1 GiB)
- `nats.stream.replicas`: JetStream replicas (default: 1; use 3 in prod)

### Producer
- `nats.topic`: topic to produce to
- `nats.producer.timeout`: publish ack timeout (default: `5s`)

### Consumer
- `nats.consumer.group_id`: durable consumer name (default: `file-convert-group`)
- `nats.consumer.topics`: list of topics to consume
- `nats.consumer.auto_offset_reset`: `earliest` (default) or `latest` — applies on first creation only
- `nats.consumer.max_deliver`: max delivery attempts (default: 5)
- `nats.consumer.ack_wait`: redelivery timeout for unacked messages (default: `30s`)
- `nats.consumer.retry_backoff`: delay before redelivery after handler error (default: `2s`)

## Usage

### Producer

```go
import gonats "github.com/paaavkata/go-nats"

producer, err := gonats.NewProducer(&gonats.ProducerConfig{
    URLs:     []string{"nats://nats.data-dev.svc:4222"},
    ClientID: "payment-service",
    Topic:    "payment-events",
})
if err != nil { ... }
defer producer.Close()

err = producer.SendMessage(appID, paymentEvent) // value is JSON-marshalled
```

### Consumer

```go
consumer, err := gonats.NewConsumer(&gonats.ConsumerConfig{
    URLs:    []string{"nats://nats.data-dev.svc:4222"},
    GroupID: "usage-service-group",
    Topics:  []string{"payment-events"},
})
if err != nil { ... }
defer consumer.Close()

err = consumer.ConsumeMessages(ctx, func(msg *gonats.Message) error {
    // return nil to ack; return error to redeliver after RetryBackoff
    return process(msg.Value)
})
```

## Migration notes (from go-kafka)

- `Brokers []string` → `URLs []string`; env `KAFKA_BOOTSTRAP_SERVER` → `NATS_URL`
  (e.g. `nats://nats.data-dev.svc:4222`).
- `EnableAutoCommit` still exists for compile compat but acks are always
  explicit (ack on success, redeliver on error) — this is an upgrade over the
  old log-and-drop behaviour; make sure handlers are idempotent.
- `MaxRetries`/`RetryMax` (client-side) → `MaxDeliver` (server-side redelivery).
- No `RequiredAcks`/compression/batch knobs: publishes are synchronous and
  server-acked (Kafka `acks=all` equivalent).
