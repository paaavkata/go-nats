package gonats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Dead-letter stream: a message that exhausts MaxDeliver is copied here (with
// its original subject and headers) and then terminated on the source
// consumer, instead of being dropped silently by the server.
const (
	DeadLetterStream        = "dead-letter"
	DeadLetterSubjectPrefix = "dlq."
	DeadLetterMaxAge        = 14 * 24 * time.Hour
	DeadLetterMaxBytes      = 256 << 20 // 256 MiB

	DlqReasonHeader   = "Dlq-Reason"
	DlqStreamHeader   = "Dlq-Stream"
	DlqConsumerHeader = "Dlq-Consumer"
	DlqSubjectHeader  = "Dlq-Original-Subject"
	DlqSequenceHeader = "Dlq-Original-Sequence"
	// DlqMsgIDHeader keeps the original Nats-Msg-Id, which the copy replaces
	// with its own dedup id.
	DlqMsgIDHeader = "Dlq-Original-Msg-Id"
)

// DeadLetterSubject returns the dead-letter subject for an original subject.
func DeadLetterSubject(original string) string {
	return DeadLetterSubjectPrefix + original
}

// EnsureDeadLetterStream creates the shared dead-letter stream if it does not
// exist. Replicas follow the business-stream rule (cfg.Replicas, resolved from
// NATS_STREAM_REPLICAS / ENV). An existing stream is left unchanged.
func EnsureDeadLetterStream(ctx context.Context, js jetstream.JetStream, cfg StreamConfig) error {
	applyStreamDefaults(&cfg)
	_, err := js.Stream(ctx, DeadLetterStream)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("failed to look up stream %q: %w", DeadLetterStream, err)
	}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      DeadLetterStream,
		Subjects:  []string{DeadLetterSubjectPrefix + ">"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardOld,
		MaxAge:    DeadLetterMaxAge,
		MaxBytes:  DeadLetterMaxBytes,
		Replicas:  cfg.Replicas,
	})
	if err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return fmt.Errorf("failed to create stream %q: %w", DeadLetterStream, err)
	}
	return nil
}

// deadLetter copies msg to the dead-letter stream. The copy keeps the original
// headers (Trace-Id included) and adds the Dlq-* headers; the original
// Nats-Msg-Id moves to Dlq-Original-Msg-Id. It does not ack or term the original.
func (c *Consumer) deadLetter(ctx context.Context, sub subscription, msg jetstream.Msg, reason error) error {
	hdr := nats.Header{}
	for k, v := range msg.Headers() {
		hdr[k] = append([]string(nil), v...)
	}
	hdr.Set(DlqReasonHeader, truncate(reason.Error(), 512))
	hdr.Set(DlqStreamHeader, sub.stream)
	hdr.Set(DlqConsumerHeader, sub.durable)
	hdr.Set(DlqSubjectHeader, msg.Subject())
	if id := msg.Headers().Get(MsgIDHeader); id != "" {
		hdr.Set(DlqMsgIDHeader, id)
	}
	if meta, err := msg.Metadata(); err == nil {
		hdr.Set(DlqSequenceHeader, fmt.Sprintf("%d", meta.Sequence.Stream))
		// One DLQ entry per original stream sequence, even if two pods race.
		hdr.Set(MsgIDHeader, fmt.Sprintf("dlq-%s-%d", sub.stream, meta.Sequence.Stream))
	}
	_, err := c.js.PublishMsg(ctx, &nats.Msg{
		Subject: DeadLetterSubject(msg.Subject()),
		Header:  hdr,
		Data:    msg.Data(),
	})
	return err
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
