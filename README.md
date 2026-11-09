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
configuration (externally managed configuration wins) with two exceptions:
missing app-scoped subjects (`<base>.*`) are **added** (never removed), and
request streams (`job`) are switched to `DiscardNew` (see v0.6.0 changes). A
replica-count mismatch on an existing stream is **tolerated and logged**, never
changed; scale existing streams operationally
(`nats stream edit <name> --replicas 3`).

## Delivery semantics

At-least-once. Handler returns `nil` → ack. Handler returns error → redelivered
after a jittered exponential delay, up to `MaxDeliver` attempts, then the
message is dead-lettered. By default messages within one topic are processed
serially (preserves order); different topics are consumed concurrently.

## v0.6.0 changes

- **Group ID required**: `NewConsumer` returns an error when `GroupID` is
  empty. The old `file-convert-group` default (`DefaultConsumerGroupID`) is
  removed, so two services can no longer share a durable by accident.
- **Concurrency** (`ConsumerConfig.Concurrency`, `nats.consumer.concurrency`):
  `0`/`1` (default) keeps the serial inline loop. `N > 1` runs up to N handlers
  at once per topic, each message acked or naked on its own; in-topic order is
  then not guaranteed, so keep `1` for consumers that need it.
  `MaxAckPending` (`nats.consumer.max_ack_pending`) defaults to `Concurrency`
  when `Concurrency > 1`, otherwise the server default (1000) applies.
- **Durable reconciliation**: at start an existing durable whose `AckWait`,
  `MaxDeliver`, `InactiveThreshold` or `MaxAckPending` differs from config is
  updated (`UpdateConsumer`). Immutable settings (`DeliverPolicy`,
  `FilterSubject`) are never changed; a mismatch is only logged. A
  `MaxAckPending` of 0 in config is not compared.
- **Backoff**: the Nak delay is `min(RetryBackoff << (NumDelivered-1),
  MaxRetryBackoff) * U(0.5, 1.5)`. With the defaults (2s base, 60s cap) failed
  deliveries 1, 2, 3, 4, 5 wait about 2, 4, 8, 16, 32 s (each between half and
  one and a half times that), so time to dead-letter with `MaxDeliver` 5 is
  about 30 s instead of 8 s. Lower `RetryBackoff` for faster retries.
- **Request streams discard new**: streams for `RequestTopics` (`job`) are
  created with `DiscardNew`, and an existing one with another policy is
  updated. A full `job` stream now rejects the publish with an error instead
  of silently evicting the oldest unprocessed request. Operator revert:
  `nats stream edit job --discard old` (reapplied on the next service start
  while `job` stays in `RequestTopics`, so a lasting revert needs a library
  change).
- **Trace id**: a publish copies `ctx.Value("trace_id")` (the key go-server's
  request-id middleware sets) into the `Trace-Id` header. Consumers read it as
  `Message.TraceID`; `msg.Context(ctx)` returns ctx carrying it under
  `"trace_id"` so go-logger `*Ctx` calls log it. No trace id in ctx means no
  header and a context without one.
- **Dead-letter copies** keep all original headers including `Trace-Id`; the
  original `Nats-Msg-Id` moves to `Dlq-Original-Msg-Id` because the copy uses
  its own dedup id.

## Resilience (v0.5.0)

- **Dead-letter**: a message whose handler fails on its last delivery
  (`NumDelivered >= MaxDeliver`) is copied to stream `dead-letter`, subject
  `dlq.<original subject>`, with the original headers plus `Dlq-Reason`,
  `Dlq-Stream`, `Dlq-Consumer`, `Dlq-Original-Subject`, `Dlq-Original-Sequence`,
  then terminated on the source consumer. The stream is created on first
  consumer start (`EnsureDeadLetterStream`): Limits retention, 14d / 256 MiB,
  replicas by the same `ENV` / `NATS_STREAM_REPLICAS` rule as business
  streams. `MaxDeliver <= 0` (unlimited) never dead-letters. Replay a message
  with `nats stream get dead-letter <seq>` and republish to the original subject.
- **Panic isolation**: a handler panic is recovered, logged with its stack and
  counted as a handler error (Nak, then dead-letter on the last attempt). The
  consume loop keeps running.
- **Loop health**: `Consumer.Healthy(ctx) error` returns the error that made
  `ConsumeMessages` exit unexpectedly (iterator failure, consumer deleted
  server-side), nil while running or after `Close`. Wire it into readiness:
  `mgr.AddReadinessCheck("nats-consumer", consumer.Healthy)`.
- **Metrics**: `Consumer.RegisterMetrics(prometheus.Registerer)` exposes
  `gonats_dead_letter_total{topic}` and `gonats_handler_panics_total{topic}`.
- Fixed in the same release: a dead iterator used to hang the topic goroutine
  forever (the stop watcher waited for a ctx cancel that only happened after the
  loop returned), so the pod stayed Ready with no consumer.

## Configuration (Viper keys)

### Connection (shared)
- `nats.urls`: NATS server URLs (default: `["nats://localhost:4222"]`)
- `nats.client_id`: connection name shown in NATS monitoring
- `nats.tls.enabled|ca_file|cert_file|key_file|insecure_skip_verify`
- `nats.auth.username|password|token|creds_file`
- Environment fallback (since v0.7.0): when the caller passes no `Auth` (or an
  empty one), `connect` reads `NATS_CREDS_FILE`, then `NATS_TOKEN`, then
  `NATS_USER`/`NATS_PASSWORD`. Platform services set `NATS_USER=<service>` in
  `envVars` and `NATS_PASSWORD` in `secretEnvVars` from the namespace's
  `nats-credentials` SealedSecret; no code change is needed. `AuthConfigFromEnv()`
  exposes the same lookup.

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
- `nats.consumer.group_id`: durable consumer name (required, no default)
- `nats.consumer.topics`: list of topics to consume
- `nats.consumer.auto_offset_reset`: `earliest` (default) or `latest` — applies on first creation only
- `nats.consumer.max_deliver`: max delivery attempts (default: 5)
- `nats.consumer.ack_wait`: redelivery timeout for unacked messages (default: `30s`)
- `nats.consumer.retry_backoff`: base delay before redelivery after handler error, doubled per delivery and jittered (default: `2s`)
- `nats.consumer.max_retry_backoff`: cap on that delay before jitter (default: `60s`)
- `nats.consumer.concurrency`: handlers running at once per topic (default: `1`, serial)
- `nats.consumer.max_ack_pending`: server-side cap on unacked messages (default: `concurrency` when > 1, else server default)
- `nats.consumer.inactive_threshold`: auto-delete an idle durable after this long (default: never)

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
    // return nil to ack; return error to redeliver after the backoff
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
