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
service startup order doesn't matter. Existing streams keep their
configuration — externally managed configuration wins — with one exception:
missing app-scoped subjects (`<base>.*`) are **added** (never removed). A
replica-count mismatch on an existing stream is **tolerated and logged**, never
changed; scale existing streams operationally
(`nats stream edit <name> --replicas 3`).

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
- `nats.stream.max_bytes`: size cap per stream (default: 128 MiB; reserved up front against the server's `max_file_store`, so keep the fleet-wide sum below it)
- `nats.stream.replicas` / `StreamConfig.Replicas`: explicit replicas. When
  zero (the norm), resolved from env: `NATS_STREAM_REPLICAS` if set (1..5;
  invalid values are logged and ignored), else `3` when `ENV=prod`, else `1`.
  `ENV` is stamped on every pod by the service helm chart.

Operational log lines (subjects added, replica mismatch) go through the
standard `log` package unless `gonats.SetLogger(logger.Infof)` is called.

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

## App-scoped subjects

`job`, `job-status-updates`, `job-status-changed`, `audit-events` and
`send-email` (`gonats.AppScopedTopics`) are published per app on
`<base>.<app_id>`. The stream stays one per base (same name as before) with
subjects `[<base>, <base>.*]`, so legacy flat and scoped messages share it.
Other topics stay flat unless a producer sets `AppScoped: true` / a consumer
uses a scoped subscription.

| Mode      | How                                   | Durable(s)                   | Filter            |
|-----------|---------------------------------------|------------------------------|-------------------|
| legacy    | `ConsumerConfig.Topics`               | `<group>`                    | `<base>`          |
| one app   | `gonats.SubscribeApp(base, appID)`    | `<group>_<appID>`            | `<base>.<appID>`  |
| all apps  | `gonats.SubscribeAllApps(base)`       | `<group>` + `<group>__all`   | `<base>` + `<base>.*` |

All-apps is two durables, not one with two filter subjects: widening the
legacy durable in place would skip scoped messages it already scanned past
(verified against nats 2.11), and a new two-filter durable would replay the
legacy backlog. `Message.Topic` is always the base; `Message.Subject` /
`Message.AppID` carry the concrete subject and its app (`""` = legacy flat).

```go
// producer (stream already scoped for AppScopedTopics bases)
p, _ := gonats.NewProducer(&gonats.ProducerConfig{URLs: urls, ClientID: "conversion-service", Topic: "job"})
err := p.SendScoped(ctx, appID, "", payload)                 // -> job.<appID>
err = p.SendScopedWithMsgID(ctx, appID, key, eventUID, ev)   // + Nats-Msg-Id dedup

// consumer: one app / all apps
c, _ := gonats.NewConsumer(&gonats.ConsumerConfig{URLs: urls, GroupID: "svc-group",
    Subscriptions: []gonats.TopicSubscription{gonats.SubscribeAllApps("job")}})
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
