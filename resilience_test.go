package gonats

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// --- unit: Healthy / safeHandle / lastDelivery (no server) ---

// fakeIterator returns one error from Next, simulating a consumer deleted
// server-side or any iterator failure.
type fakeIterator struct{ err error }

func (f *fakeIterator) Next() (jetstream.Msg, error) { return nil, f.err }
func (f *fakeIterator) Stop()                        {}
func (f *fakeIterator) Drain()                       {}

type fakeConsumer struct {
	jetstream.Consumer // nil: any other method panics, which a test would surface
	it                 *fakeIterator
}

func (f *fakeConsumer) Messages(...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	return f.it, nil
}

func TestHealthyReflectsLoopExit(t *testing.T) {
	c := &Consumer{config: &ConsumerConfig{}}
	applyConsumerDefaults(c.config)
	if err := c.Healthy(context.Background()); err != nil {
		t.Fatalf("fresh consumer must be healthy, got %v", err)
	}

	sub := subscription{topic: "t", stream: "t", durable: "g"}
	boom := errors.New("consumer deleted")
	err := c.consumeTopic(context.Background(), sub, &fakeConsumer{it: &fakeIterator{err: boom}}, func(*Message) error { return nil })
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("consumeTopic must surface the iterator error, got %v", err)
	}
	// ConsumeMessages records the exit; emulate that step.
	c.setLoopErr(err)
	if got := c.Healthy(context.Background()); got == nil || !strings.Contains(got.Error(), "consumer deleted") {
		t.Fatalf("Healthy after loop death = %v", got)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if got := c.Healthy(context.Background()); got != nil {
		t.Fatalf("Healthy after Close must be nil (clean shutdown), got %v", got)
	}
}

func TestSafeHandleRecoversPanic(t *testing.T) {
	c := &Consumer{config: &ConsumerConfig{}}
	reg := prometheus.NewRegistry()
	if err := c.RegisterMetrics(reg); err != nil {
		t.Fatal(err)
	}
	err := c.safeHandle("t", func(*Message) error { panic("poison") }, &Message{Topic: "t", Subject: "t"})
	if err == nil || !strings.Contains(err.Error(), "poison") {
		t.Fatalf("panic must become an error, got %v", err)
	}
	if n := testutil.ToFloat64(c.handlerPanic.WithLabelValues("t")); n != 1 {
		t.Fatalf("gonats_handler_panics_total = %v, want 1", n)
	}
	if err := c.safeHandle("t", func(*Message) error { return nil }, &Message{}); err != nil {
		t.Fatalf("plain handler must pass through, got %v", err)
	}
}

func TestLastDeliveryUnlimitedNeverTrue(t *testing.T) {
	c := &Consumer{config: &ConsumerConfig{MaxDeliver: -1}}
	if c.lastDelivery(nil) {
		t.Fatal("MaxDeliver -1 must never dead-letter")
	}
}

// --- integration: dead-letter and panic paths against a real server ---

func TestIntegrationDeadLetterAfterMaxDeliver(t *testing.T) {
	url, js := testServer(t)
	t.Setenv(EnvStreamReplicas, "")
	t.Setenv(EnvEnvironment, "")
	topic := uniqueTopic(t, js)
	ctx := context.Background()
	t.Cleanup(func() { _ = js.DeleteStream(ctx, DeadLetterStream) })

	prod, err := NewProducer(&ProducerConfig{URLs: []string{url}, Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	if err := prod.SendMessage("k", payload{1}); err != nil {
		t.Fatal(err)
	}

	cons, err := NewConsumer(&ConsumerConfig{
		URLs: []string{url}, GroupID: "dlq-grp", Topics: []string{topic},
		MaxDeliver: 2, RetryBackoff: 50 * time.Millisecond, AckWait: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	reg := prometheus.NewRegistry()
	if err := cons.RegisterMetrics(reg); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() {
		_ = cons.ConsumeMessages(runCtx, func(*Message) error {
			attempts.Add(1)
			return errors.New("always fails")
		})
	}()

	dlq := waitForDLQ(t, js, 1)
	cancel()

	if got := attempts.Load(); got != 2 {
		t.Fatalf("handler attempts = %d, want 2 (MaxDeliver)", got)
	}
	msg := dlq[0]
	if msg.Subject != DeadLetterSubject(topic) {
		t.Fatalf("dlq subject = %q", msg.Subject)
	}
	for _, h := range []string{DlqReasonHeader, DlqStreamHeader, DlqConsumerHeader, DlqSubjectHeader, DlqSequenceHeader} {
		if msg.Header.Get(h) == "" {
			t.Fatalf("dlq header %s missing", h)
		}
	}
	if !strings.Contains(msg.Header.Get(DlqReasonHeader), "always fails") {
		t.Fatalf("dlq reason = %q", msg.Header.Get(DlqReasonHeader))
	}
	if n := testutil.ToFloat64(cons.deadLetters.WithLabelValues(topic)); n != 1 {
		t.Fatalf("gonats_dead_letter_total = %v, want 1", n)
	}
	// Source consumer has nothing pending: the message was terminated.
	info, err := js.Consumer(ctx, StreamName(topic), DurableName("dlq-grp", ""))
	if err != nil {
		t.Fatal(err)
	}
	ci, err := info.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ci.NumPending != 0 || ci.NumAckPending != 0 {
		t.Fatalf("source consumer pending=%d ackPending=%d, want 0/0", ci.NumPending, ci.NumAckPending)
	}
	if di := streamInfoByName(t, js, DeadLetterStream); di.Config.MaxBytes != DeadLetterMaxBytes || di.Config.MaxAge != DeadLetterMaxAge {
		t.Fatalf("dead-letter limits = %d/%s", di.Config.MaxBytes, di.Config.MaxAge)
	}
}

func TestIntegrationPanicReachesDLQ(t *testing.T) {
	url, js := testServer(t)
	t.Setenv(EnvStreamReplicas, "")
	t.Setenv(EnvEnvironment, "")
	topic := uniqueTopic(t, js)
	ctx := context.Background()
	t.Cleanup(func() { _ = js.DeleteStream(ctx, DeadLetterStream) })

	prod, err := NewProducer(&ProducerConfig{URLs: []string{url}, Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	if err := prod.SendMessage("k", payload{1}); err != nil {
		t.Fatal(err)
	}
	if err := prod.SendMessage("k", payload{2}); err != nil {
		t.Fatal(err)
	}

	cons, err := NewConsumer(&ConsumerConfig{
		URLs: []string{url}, GroupID: "panic-grp", Topics: []string{topic},
		MaxDeliver: 2, RetryBackoff: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()

	var good atomic.Int32
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- cons.ConsumeMessages(runCtx, func(m *Message) error {
			if strings.Contains(string(m.Value), `"n":1`) {
				panic("poison message")
			}
			good.Add(1)
			return nil
		})
	}()

	waitForDLQ(t, js, 1)
	deadline := time.Now().Add(5 * time.Second)
	for good.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("consumer must survive a panic, exited with %v", err)
	}
	if good.Load() != 1 {
		t.Fatalf("second message not processed after panic: good=%d", good.Load())
	}
}

func waitForDLQ(t *testing.T, js jetstream.JetStream, want int) []*jetstream.RawStreamMsg {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		s, err := js.Stream(ctx, DeadLetterStream)
		if err == nil {
			info, _ := s.Info(ctx)
			if info != nil && int(info.State.Msgs) >= want {
				var out []*jetstream.RawStreamMsg
				for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
					m, err := s.GetMsg(ctx, seq)
					if err == nil {
						out = append(out, m)
					}
				}
				return out
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("dead-letter stream did not receive %d message(s) in time", want)
	return nil
}

func streamInfoByName(t *testing.T, js jetstream.JetStream, name string) *jetstream.StreamInfo {
	t.Helper()
	s, err := js.Stream(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return s.CachedInfo()
}
