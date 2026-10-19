package gonats

import (
	"context"
	"testing"
	"time"
)

// The 2026-10-18 incident: JetStream lost quorum for ~15s, every pull
// iterator returned "no heartbeat received", ConsumeMessages exited and the
// pods stayed unready until a manual restart. The consumer must rebuild the
// iterator (and the durable, if it is gone) and keep consuming.
func TestIntegrationConsumerRecoversFromDeletedDurable(t *testing.T) {
	url, js := testServer(t)
	t.Setenv(EnvStreamReplicas, "")
	t.Setenv(EnvEnvironment, "")
	topic := uniqueTopic(t, js)
	ctx := context.Background()

	prod, err := NewProducer(&ProducerConfig{URLs: []string{url}, Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	cons, err := NewConsumer(&ConsumerConfig{
		URLs: []string{url}, GroupID: "recover-grp", Topics: []string{topic},
		RetryBackoff: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()

	got := make(chan *Message, 16)
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	exited := make(chan error, 1)
	go func() {
		exited <- cons.ConsumeMessages(runCtx, func(m *Message) error { got <- m; return nil })
	}()

	if err := prod.SendMessage("k", payload{1}); err != nil {
		t.Fatal(err)
	}
	waitMsg(t, got, "first message")

	// Kill the durable under the running iterator.
	if err := js.DeleteConsumer(ctx, StreamName(topic), DurableName("recover-grp", "")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for cons.Healthy(ctx) == nil {
		if time.Now().After(deadline) {
			t.Fatal("Healthy never reported the broken iterator")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := prod.SendMessage("k", payload{2}); err != nil {
		t.Fatal(err)
	}
	// The recreated durable replays from the start; drain until payload 2 shows up.
	for {
		m := waitMsg(t, got, "message after recovery")
		if string(m.Value) == `{"n":2}` {
			break
		}
	}
	if err := cons.Healthy(ctx); err != nil {
		t.Fatalf("Healthy after recovery = %v, want nil", err)
	}
	select {
	case err := <-exited:
		t.Fatalf("ConsumeMessages exited early: %v", err)
	default:
	}
	cancel()
	if err := <-exited; err != nil {
		t.Fatalf("ConsumeMessages on shutdown = %v, want nil", err)
	}
}

func waitMsg(t *testing.T, got <-chan *Message, what string) *Message {
	t.Helper()
	select {
	case m := <-got:
		return m
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}
