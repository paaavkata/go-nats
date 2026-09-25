package gonats

import (
	"fmt"
	"strings"
)

// App-scoped subjects.
//
// A handful of platform topics carry traffic for several apps. For those, the
// subject is "<base>.<app_id>" (e.g. "job.fileconvert", "audit-events.scantinel")
// while the JetStream stream stays one-per-base (name == sanitized base) and
// captures both the legacy flat subject "<base>" and "<base>.*". Legacy and
// scoped messages therefore land in the same stream during cutover and nothing
// is stranded.
//
// Consumers pick one of three modes per base topic (see TopicSubscription):
//
//	legacy    ConsumerConfig.Topics        durable "<group>"         filter "<base>"
//	one app   SubscribeApp(base, appID)    durable "<group>_<appID>" filter "<base>.<appID>"
//	all apps  SubscribeAllApps(base)       durable "<group>"         filter "<base>"   (legacy, reused)
//	                                       durable "<group>__all"    filter "<base>.*"
//
// All-apps is two durables rather than one with two filter subjects; see
// SubscribeAllApps for why.

// AppScopedTopics lists the platform base topics that are app-scoped. Streams
// for these bases always include "<base>.*", whichever service (scoped or
// legacy) happens to create the stream first, so the stream shape never depends
// on startup order. Any other topic becomes scoped only when a producer sets
// ProducerConfig.AppScoped or a consumer subscribes with SubscribeApp /
// SubscribeAllApps.
var AppScopedTopics = []string{
	"job",
	"job-status-updates",
	"job-status-changed",
	"audit-events",
	"send-email",
}

// IsAppScopedTopic reports whether base is one of AppScopedTopics.
func IsAppScopedTopic(base string) bool {
	for _, t := range AppScopedTopics {
		if t == base {
			return true
		}
	}
	return false
}

// DurableAppSeparator joins the group ID and the app ID in the durable name of
// a single-app consumer ("<group>_<appID>"). Existing group IDs use '-' only,
// so the '_' keeps the app-suffixed durable distinct from the legacy one.
const DurableAppSeparator = "_"

// AllAppsDurableSuffix is appended to the group ID for the scoped half of an
// all-apps subscription ("<group>__all"). App IDs may not start with '_', so
// it can never equal a single-app durable.
const AllAppsDurableSuffix = "__all"

// ValidateAppID checks that appID can be used as a single NATS subject token
// and as a durable-name suffix: non-empty, not starting with '_', and free of
// '.', '*', '>', path separators and whitespace.
func ValidateAppID(appID string) error {
	if appID == "" {
		return fmt.Errorf("app id is required")
	}
	if strings.HasPrefix(appID, "_") {
		return fmt.Errorf("app id %q must not start with '_'", appID)
	}
	if i := strings.IndexFunc(appID, func(r rune) bool {
		switch r {
		case '.', '*', '>', '/', '\\':
			return true
		}
		return r <= ' ' || r == 0x7f
	}); i >= 0 {
		return fmt.Errorf("app id %q contains invalid character %q", appID, appID[i])
	}
	return nil
}

// validateBaseTopic checks that base is usable as the base of a scoped subject.
func validateBaseTopic(base string) error {
	if base == "" {
		return fmt.Errorf("topic is required")
	}
	if strings.ContainsAny(base, "*> \t\r\n") || strings.HasPrefix(base, ".") || strings.HasSuffix(base, ".") {
		return fmt.Errorf("topic %q is not a valid base subject", base)
	}
	return nil
}

// ScopedSubject returns the app-scoped subject "<base>.<appID>".
func ScopedSubject(base, appID string) (string, error) {
	if err := validateBaseTopic(base); err != nil {
		return "", err
	}
	if err := ValidateAppID(appID); err != nil {
		return "", fmt.Errorf("scoped subject for %q: %w", base, err)
	}
	return base + "." + appID, nil
}

// AllAppsSubjects returns the subjects covering every app on base: the legacy
// flat subject and the single-token wildcard (the stream subjects of a scoped
// topic).
func AllAppsSubjects(base string) []string {
	return []string{base, base + ".*"}
}

// StreamSubjects returns the subjects the stream for base is created with:
// just base for a flat topic, base plus "<base>.*" for a scoped one.
func StreamSubjects(base string, scoped bool) []string {
	if scoped {
		return AllAppsSubjects(base)
	}
	return []string{base}
}

// StreamName returns the JetStream stream name backing base.
func StreamName(base string) string {
	return sanitizeName(base)
}

// DurableName returns the durable consumer name for groupID on one base topic.
// An empty appID yields the legacy durable ("<group>", also the flat half of
// an all-apps subscription); a non-empty appID yields the single-app durable
// ("<group>_<appID>").
func DurableName(groupID, appID string) string {
	if appID == "" {
		return sanitizeName(groupID)
	}
	return sanitizeName(groupID) + DurableAppSeparator + sanitizeName(appID)
}

// AllAppsDurableName returns the durable of the scoped half of an all-apps
// subscription: "<group>__all".
func AllAppsDurableName(groupID string) string {
	return sanitizeName(groupID) + AllAppsDurableSuffix
}

// AppIDFromSubject extracts the app ID from a subject on base: "" for the
// legacy flat subject (or any subject not under base), otherwise the token
// after "<base>.".
func AppIDFromSubject(base, subject string) string {
	if rest, ok := strings.CutPrefix(subject, base+"."); ok {
		return rest
	}
	return ""
}
