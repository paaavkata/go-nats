package gonats

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Integration tests run against a real JetStream server and are skipped unless
// NATS_TEST_URL is set (and never under -short), e.g.:
//
//	docker run -d --rm -p 14222:4222 nats:2.11-alpine -js
//	NATS_TEST_URL=nats://localhost:14222 go test ./...
func testServer(t *testing.T) (string, jetstream.JetStream) {
	t.Helper()
	url := os.Getenv("NATS_TEST_URL")
	if url == "" || testing.Short() {
		t.Skip("NATS_TEST_URL not set")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return url, js
}

// uniqueTopic returns a fresh base topic and deletes its stream afterwards.
func uniqueTopic(t *testing.T, js jetstream.JetStream) string {
	t.Helper()
	topic := fmt.Sprintf("it-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), StreamName(topic)) })
	return topic
}

func streamInfo(t *testing.T, js jetstream.JetStream, topic string) *jetstream.StreamInfo {
	t.Helper()
	s, err := js.Stream(context.Background(), StreamName(topic))
	if err != nil {
		t.Fatalf("stream %q: %v", topic, err)
	}
	return s.CachedInfo()
}

type payload struct {
	N int `json:"n"`
}

// collect consumes until want messages arrived, then cancels.
func collect(t *testing.T, c *Consumer, want int) []*Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var (
		mu  sync.Mutex
		got []*Message
	)
	err := c.ConsumeMessages(ctx, func(m *Message) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, m)
		if len(got) >= want {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ConsumeMessages: %v", err)
	}
	if len(got) != want {
		t.Fatalf("got %d messages, want %d", len(got), want)
	}
	return got
}

func subjectsOf(msgs []*Message) []string {
	var out []string
	for _, m := range msgs {
		out = append(out, m.Subject+"|"+m.AppID+"|"+m.Topic)
	}
	sort.Strings(out)
	return out
}

func TestIntegrationScopedFlow(t *testing.T) {
	url, js := testServer(t)
	t.Setenv(EnvStreamReplicas, "")
	t.Setenv(EnvEnvironment, "")
	topic := uniqueTopic(t, js)
	ctx := context.Background()

	// 1. A legacy producer creates the legacy single-subject stream and
	//    publishes a flat message; a legacy consumer acks it.
	legacy, err := NewProducer(&ProducerConfig{URLs: []string{url}, Topic: topic})
	if err != nil {
		t.Fatalf("legacy producer: %v", err)
	}
	defer legacy.Close()
	if got := streamInfo(t, js, topic).Config.Subjects; !slices.Equal(got, []string{topic}) {
		t.Fatalf("legacy stream subjects = %v", got)
	}
	if err := legacy.SendScoped(ctx, "fileconvert", "", payload{0}); err == nil {
		t.Fatal("SendScoped on a non-scoped producer must fail")
	}
	if err := legacy.SendMessage("k", payload{1}); err != nil {
		t.Fatalf("legacy send: %v", err)
	}
	legacyCons, err := NewConsumer(&ConsumerConfig{URLs: []string{url}, GroupID: "grp", Topics: []string{topic}})
	if err != nil {
		t.Fatalf("legacy consumer: %v", err)
	}
	collect(t, legacyCons, 1)
	legacyCons.Close()

	// 2. A scoped producer widens the stream in place (no data loss) and
	//    publishes scoped messages, one of them twice with the same msg id.
	scoped, err := NewProducer(&ProducerConfig{URLs: []string{url}, Topic: topic, AppScoped: true})
	if err != nil {
		t.Fatalf("scoped producer: %v", err)
	}
	defer scoped.Close()
	info := streamInfo(t, js, topic)
	if !slices.Equal(info.Config.Subjects, []string{topic, topic + ".*"}) {
		t.Fatalf("scoped stream subjects = %v", info.Config.Subjects)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream lost messages on update: %d", info.State.Msgs)
	}
	for _, app := range []string{"fileconvert", "scantinel"} {
		if err := scoped.SendScopedWithMsgID(ctx, app, "k", "id-"+app, payload{2}); err != nil {
			t.Fatalf("send scoped: %v", err)
		}
	}
	if err := scoped.SendScopedWithMsgID(ctx, "fileconvert", "k", "id-fileconvert", payload{2}); err != nil {
		t.Fatalf("duplicate send: %v", err)
	}
	if n := streamInfo(t, js, topic).State.Msgs; n != 3 {
		t.Fatalf("dedup failed: stream has %d msgs, want 3", n)
	}
	if err := legacy.SendMessage("k", payload{3}); err != nil { // one more legacy
		t.Fatalf("legacy send: %v", err)
	}
	if err := scoped.SendScoped(ctx, "bad.app", "", payload{0}); err == nil {
		t.Fatal("invalid app id must fail")
	}

	// 3. Single-app consumer sees only its app, on its own durable.
	appCons, err := NewConsumer(&ConsumerConfig{URLs: []string{url}, GroupID: "grp",
		Subscriptions: []TopicSubscription{SubscribeApp(topic, "fileconvert")}})
	if err != nil {
		t.Fatalf("app consumer: %v", err)
	}
	got := collect(t, appCons, 1)
	appCons.Close()
	if got[0].AppID != "fileconvert" || got[0].Subject != topic+".fileconvert" || got[0].Topic != topic {
		t.Fatalf("app consumer got %+v", got[0])
	}
	if _, err := js.Consumer(ctx, StreamName(topic), "grp_fileconvert"); err != nil {
		t.Fatalf("app durable missing: %v", err)
	}

	// 4. All-apps keeps the legacy durable (its acked legacy message is NOT
	//    replayed) and adds "grp__all", which picks up both scoped messages
	//    even though they were published before it existed.
	allCons, err := NewConsumer(&ConsumerConfig{URLs: []string{url}, GroupID: "grp",
		Subscriptions: []TopicSubscription{SubscribeAllApps(topic)}})
	if err != nil {
		t.Fatalf("all consumer: %v", err)
	}
	got = collect(t, allCons, 3)
	allCons.Close()
	want := []string{
		topic + "." + "fileconvert|fileconvert|" + topic,
		topic + "." + "scantinel|scantinel|" + topic,
		topic + "||" + topic,
	}
	sort.Strings(want)
	if gotS := subjectsOf(got); !slices.Equal(gotS, want) {
		t.Fatalf("all-apps got %v, want %v", gotS, want)
	}
	for _, m := range got {
		var p payload
		if err := json.Unmarshal(m.Value, &p); err != nil || p.N == 1 {
			t.Fatalf("unexpected message %s (replayed legacy?)", m.Value)
		}
	}
	for durable, filter := range map[string]string{"grp": topic, "grp__all": topic + ".*"} {
		ci, err := js.Consumer(ctx, StreamName(topic), durable)
		if err != nil {
			t.Fatalf("durable %q: %v", durable, err)
		}
		if f := ci.CachedInfo().Config.FilterSubject; f != filter {
			t.Fatalf("durable %q filter = %q, want %q", durable, f, filter)
		}
	}
}

func TestIntegrationKnownScopedTopicAndReplicas(t *testing.T) {
	url, js := testServer(t)
	ctx := context.Background()

	// A legacy producer on an AppScopedTopics base still creates the scoped
	// stream shape. Uses a throwaway name registered for the test.
	topic := uniqueTopic(t, js)
	AppScopedTopics = append(AppScopedTopics, topic)
	t.Cleanup(func() { AppScopedTopics = AppScopedTopics[:len(AppScopedTopics)-1] })

	t.Setenv(EnvEnvironment, "dev")
	t.Setenv(EnvStreamReplicas, "")
	p, err := NewProducer(&ProducerConfig{URLs: []string{url}, Topic: topic})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer p.Close()
	info := streamInfo(t, js, topic)
	if !slices.Equal(info.Config.Subjects, []string{topic, topic + ".*"}) || info.Config.Replicas != 1 {
		t.Fatalf("stream config subjects=%v replicas=%d", info.Config.Subjects, info.Config.Replicas)
	}
	if err := p.SendScoped(ctx, "platform", "", payload{1}); err != nil {
		t.Fatalf("SendScoped on known scoped topic: %v", err)
	}

	// A prod pod against the existing R1 stream tolerates the mismatch (the
	// single-node test server could not host R3 anyway): no error, unchanged.
	t.Setenv(EnvEnvironment, "prod")
	var logged []string
	SetLogger(func(f string, a ...interface{}) { logged = append(logged, fmt.Sprintf(f, a...)) })
	t.Cleanup(func() { SetLogger(nil) })
	p2, err := NewProducer(&ProducerConfig{URLs: []string{url}, Topic: topic})
	if err != nil {
		t.Fatalf("prod producer on existing R1 stream: %v", err)
	}
	defer p2.Close()
	if r := streamInfo(t, js, topic).Config.Replicas; r != 1 {
		t.Fatalf("replicas changed to %d", r)
	}
	if len(logged) == 0 {
		t.Fatal("replica mismatch was not logged")
	}
}
