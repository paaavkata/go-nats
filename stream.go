package gonats

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// Env vars that pick the replica count for newly created streams.
const (
	// EnvStreamReplicas overrides the replica count when set (1..5).
	EnvStreamReplicas = "NATS_STREAM_REPLICAS"
	// EnvEnvironment is stamped on every pod by the service helm chart
	// ("dev" / "prod"). "prod" defaults new streams to ProdStreamReplicas.
	EnvEnvironment = "ENV"

	ProdStreamReplicas = 3
	MaxStreamReplicas  = 5 // JetStream hard limit
)

var (
	logMu sync.RWMutex
	logf  = log.Printf
)

// SetLogger routes the library's operational log lines (stream reconciliation,
// replica mismatches, consumer filter upgrades) to f. nil restores the default
// (the standard library logger).
func SetLogger(f func(format string, args ...interface{})) {
	logMu.Lock()
	defer logMu.Unlock()
	if f == nil {
		f = log.Printf
	}
	logf = f
}

func logPrintf(format string, args ...interface{}) {
	logMu.RLock()
	f := logf
	logMu.RUnlock()
	f("gonats: "+format, args...)
}

// replicasFromEnv resolves the replica count for new streams:
// NATS_STREAM_REPLICAS if set and valid, else 3 when ENV=="prod", else 1.
// An invalid NATS_STREAM_REPLICAS is logged and ignored (never fatal).
func replicasFromEnv() int {
	if raw := strings.TrimSpace(os.Getenv(EnvStreamReplicas)); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n >= 1 && n <= MaxStreamReplicas {
			return n
		}
		logPrintf("ignoring invalid %s=%q (want 1..%d)", EnvStreamReplicas, raw, MaxStreamReplicas)
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv(EnvEnvironment)), "prod") {
		return ProdStreamReplicas
	}
	return DefaultStreamReplicas
}

// ensureStream idempotently makes sure the stream backing topic exists and
// captures the subjects the caller needs.
//
// Reconciliation of an existing stream is deliberately narrow and
// deterministic:
//   - Subjects: missing required subjects are ADDED (existing ones are never
//     removed), so "<base>.*" is appended to a legacy single-subject stream.
//     Adding is the only mutation this library ever makes to a stream.
//   - Everything else (replicas, retention, limits) is TOLERATED: an existing
//     stream keeps its configuration, and a replica mismatch is only logged.
//     Scaling an existing stream is an operational step
//     (`nats stream edit <name> --replicas 3`), not a library side effect.
//
// scoped requests "<topic>.*" in addition to topic. It is forced on for
// AppScopedTopics so the stream shape does not depend on who creates it. When
// the caller itself needs the scoped subjects (requireScoped), failing to add
// them is an error; otherwise (a legacy caller on an AppScopedTopics base) it is
// logged and the caller proceeds on the flat subject, as before.
func ensureStream(ctx context.Context, js jetstream.JetStream, topic string, cfg StreamConfig, requireScoped bool) error {
	applyStreamDefaults(&cfg)
	name := StreamName(topic)
	subjects := StreamSubjects(topic, requireScoped || IsAppScopedTopic(topic))

	stream, err := js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:      name,
			Subjects:  subjects,
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			Discard:   jetstream.DiscardOld,
			MaxAge:    cfg.MaxAge,
			MaxBytes:  cfg.MaxBytes,
			Replicas:  cfg.Replicas,
		})
		if err == nil {
			return nil
		}
		if !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return fmt.Errorf("failed to create stream %q: %w", name, err)
		}
		// Lost a creation race with another service; reconcile against the winner.
		stream, err = js.Stream(ctx, name)
	}
	if err != nil {
		return fmt.Errorf("failed to look up stream %q: %w", name, err)
	}

	existing := stream.CachedInfo().Config
	if existing.Replicas != cfg.Replicas {
		logPrintf("stream %q exists with replicas=%d, wanted %d; leaving it unchanged (scale it operationally)",
			name, existing.Replicas, cfg.Replicas)
	}

	missing := missingSubjects(existing.Subjects, subjects)
	if len(missing) == 0 {
		return nil
	}
	updated := existing
	updated.Subjects = append(slices.Clone(existing.Subjects), missing...)
	if _, err := js.UpdateStream(ctx, updated); err != nil {
		if requireScoped {
			return fmt.Errorf("failed to add subjects %v to stream %q: %w", missing, name, err)
		}
		logPrintf("failed to add subjects %v to stream %q (continuing on flat subject %q): %v",
			missing, name, topic, err)
		return nil
	}
	logPrintf("stream %q: added subjects %v (now %v)", name, missing, updated.Subjects)
	return nil
}

// missingSubjects returns the entries of want not already captured by any
// subject in have (exactly, or via an externally configured wildcard such as
// "job.>" covering "job.*").
func missingSubjects(have, want []string) []string {
	var missing []string
	for _, w := range want {
		covered := false
		for _, h := range have {
			if subjectCovers(h, w) {
				covered = true
				break
			}
		}
		if !covered {
			missing = append(missing, w)
		}
	}
	return missing
}

// subjectCovers reports whether every subject matched by sub is also matched
// by pattern ('*' = one token, '>' = one or more trailing tokens).
func subjectCovers(pattern, sub string) bool {
	pt := strings.Split(pattern, ".")
	st := strings.Split(sub, ".")
	for i, p := range pt {
		if p == ">" {
			return len(st) > i
		}
		if i >= len(st) {
			return false
		}
		switch {
		case p == "*":
			if st[i] == ">" {
				return false
			}
		case p != st[i]:
			return false
		}
	}
	return len(pt) == len(st)
}
