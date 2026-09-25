package gonats

import (
	"reflect"
	"strings"
	"testing"
)

func TestScopedSubject(t *testing.T) {
	cases := []struct {
		name    string
		base    string
		appID   string
		want    string
		wantErr bool
	}{
		{"job", "job", "fileconvert", "job.fileconvert", false},
		{"audit", "audit-events", "scantinel", "audit-events.scantinel", false},
		{"platform", "send-email", "platform", "send-email.platform", false},
		{"dashed app", "job-status-updates", "my-app-2", "job-status-updates.my-app-2", false},
		{"empty app", "job", "", "", true},
		{"dot in app", "job", "file.convert", "", true},
		{"star app", "job", "*", "", true},
		{"gt app", "job", ">", "", true},
		{"space in app", "job", "file convert", "", true},
		{"slash in app", "job", "a/b", "", true},
		{"backslash in app", "job", `a\b`, "", true},
		{"tab in app", "job", "a\tb", "", true},
		{"leading underscore app", "job", "_all", "", true},
		{"inner underscore app", "job", "my_app", "job.my_app", false},
		{"empty base", "", "fileconvert", "", true},
		{"wildcard base", "job.*", "fileconvert", "", true},
		{"trailing dot base", "job.", "fileconvert", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ScopedSubject(c.base, c.appID)
			if (err != nil) != c.wantErr {
				t.Fatalf("ScopedSubject(%q, %q) err=%v wantErr=%v", c.base, c.appID, err, c.wantErr)
			}
			if got != c.want {
				t.Errorf("ScopedSubject(%q, %q) = %q, want %q", c.base, c.appID, got, c.want)
			}
		})
	}
}

func TestDurableName(t *testing.T) {
	cases := []struct {
		group, appID, want string
	}{
		{"job-service-group", "", "job-service-group"},
		{"job-service-group", "fileconvert", "job-service-group_fileconvert"},
		{"email-service-group", "scantinel", "email-service-group_scantinel"},
		{"my.group", "platform", "my-group_platform"},
		{"conversion-status-service-pod-abc", "fileconvert", "conversion-status-service-pod-abc_fileconvert"},
		{"all", "x", "all_x"},
	}
	for _, c := range cases {
		if got := DurableName(c.group, c.appID); got != c.want {
			t.Errorf("DurableName(%q, %q) = %q, want %q", c.group, c.appID, got, c.want)
		}
		// The app-suffixed durable must never equal the legacy durable.
		if c.appID != "" && DurableName(c.group, c.appID) == DurableName(c.group, "") {
			t.Errorf("app durable collides with legacy durable for %q", c.group)
		}
	}
	if got := AllAppsDurableName("event-service-group"); got != "event-service-group__all" {
		t.Errorf("AllAppsDurableName = %q", got)
	}
	// "<group>__all" is unreachable as a single-app durable: "_all" is not a valid app id.
	if ValidateAppID("_all") == nil {
		t.Error("app id \"_all\" must be rejected")
	}
}

func TestStreamSubjects(t *testing.T) {
	cases := []struct {
		base   string
		scoped bool
		want   []string
	}{
		{"job", true, []string{"job", "job.*"}},
		{"audit-events", true, []string{"audit-events", "audit-events.*"}},
		{"payment-events", false, []string{"payment-events"}},
		{"scantinel.scan-events", false, []string{"scantinel.scan-events"}},
	}
	for _, c := range cases {
		if got := StreamSubjects(c.base, c.scoped); !reflect.DeepEqual(got, c.want) {
			t.Errorf("StreamSubjects(%q, %v) = %v, want %v", c.base, c.scoped, got, c.want)
		}
	}
	if got := StreamName("scantinel.scan-events"); got != "scantinel-scan-events" {
		t.Errorf("StreamName = %q", got)
	}
}

func TestIsAppScopedTopic(t *testing.T) {
	for _, s := range []string{"job", "job-status-updates", "job-status-changed", "audit-events", "send-email"} {
		if !IsAppScopedTopic(s) {
			t.Errorf("%q should be app-scoped", s)
		}
	}
	for _, s := range []string{"k8s-scheduler-jobs", "payment-events", "usage-events", "cms-events",
		"conversion-status-updates", "scantinel.scan-events", "job.fileconvert", ""} {
		if IsAppScopedTopic(s) {
			t.Errorf("%q should be flat", s)
		}
	}
}

func TestAppIDFromSubject(t *testing.T) {
	cases := []struct{ base, subject, want string }{
		{"job", "job", ""},
		{"job", "job.fileconvert", "fileconvert"},
		{"job", "job-status-updates.fileconvert", ""},
		{"audit-events", "audit-events.scantinel", "scantinel"},
	}
	for _, c := range cases {
		if got := AppIDFromSubject(c.base, c.subject); got != c.want {
			t.Errorf("AppIDFromSubject(%q, %q) = %q, want %q", c.base, c.subject, got, c.want)
		}
	}
}

func TestReplicasFromEnv(t *testing.T) {
	cases := []struct {
		name     string
		replicas string // "" = unset
		env      string
		want     int
	}{
		{"nothing set", "", "", 1},
		{"dev", "", "dev", 1},
		{"prod", "", "prod", 3},
		{"prod uppercase", "", "PROD", 3},
		{"staging", "", "staging", 1},
		{"override wins in prod", "1", "prod", 1},
		{"override in dev", "3", "dev", 3},
		{"override padded", " 5 ", "", 5},
		{"invalid falls back to prod", "three", "prod", 3},
		{"zero falls back", "0", "dev", 1},
		{"too many falls back", "7", "prod", 3},
		{"negative falls back", "-1", "", 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(EnvStreamReplicas, c.replicas)
			t.Setenv(EnvEnvironment, c.env)
			if got := replicasFromEnv(); got != c.want {
				t.Errorf("replicasFromEnv() = %d, want %d", got, c.want)
			}
		})
	}
}

func TestApplyStreamDefaultsReplicas(t *testing.T) {
	t.Setenv(EnvStreamReplicas, "")
	t.Setenv(EnvEnvironment, "prod")

	cfg := StreamConfig{}
	applyStreamDefaults(&cfg)
	if cfg.Replicas != ProdStreamReplicas {
		t.Errorf("env-derived replicas = %d, want %d", cfg.Replicas, ProdStreamReplicas)
	}

	explicit := StreamConfig{Replicas: 1}
	applyStreamDefaults(&explicit)
	if explicit.Replicas != 1 {
		t.Errorf("explicit replicas overwritten: %d", explicit.Replicas)
	}
}

func TestMissingSubjects(t *testing.T) {
	cases := []struct {
		name       string
		have, want []string
		missing    []string
	}{
		{"legacy stream", []string{"job"}, []string{"job", "job.*"}, []string{"job.*"}},
		{"already scoped", []string{"job", "job.*"}, []string{"job", "job.*"}, nil},
		{"order irrelevant", []string{"job.*", "job"}, []string{"job", "job.*"}, nil},
		{"covered by gt", []string{"job", "job.>"}, []string{"job", "job.*"}, nil},
		{"covered by full wildcard", []string{">"}, []string{"job", "job.*"}, nil},
		{"flat wanted", []string{"payment-events"}, []string{"payment-events"}, nil},
		{"star does not cover gt", []string{"job", "job.*"}, []string{"job.>"}, []string{"job.>"}},
		{"literal does not cover star", []string{"job", "job.fileconvert"}, []string{"job.*"}, []string{"job.*"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := missingSubjects(c.have, c.want)
			if !reflect.DeepEqual(got, c.missing) {
				t.Errorf("missingSubjects(%v, %v) = %v, want %v", c.have, c.want, got, c.missing)
			}
		})
	}
}

func TestResolveSubscriptions(t *testing.T) {
	cfg := &ConsumerConfig{
		GroupID: "svc-group",
		Topics:  []string{"k8s-scheduler-jobs"},
		Subscriptions: []TopicSubscription{
			SubscribeApp("job", "fileconvert"),
			SubscribeAllApps("audit-events"),
		},
	}
	subs, err := resolveSubscriptions(cfg)
	if err != nil {
		t.Fatalf("resolveSubscriptions: %v", err)
	}
	want := []subscription{
		{topic: "k8s-scheduler-jobs", stream: "k8s-scheduler-jobs", durable: "svc-group",
			filter: "k8s-scheduler-jobs"},
		{topic: "job", stream: "job", durable: "svc-group_fileconvert",
			filter: "job.fileconvert", scoped: true},
		{topic: "audit-events", stream: "audit-events", durable: "svc-group",
			filter: "audit-events", scoped: true},
		{topic: "audit-events", stream: "audit-events", durable: "svc-group__all",
			filter: "audit-events.*", scoped: true},
	}
	if !reflect.DeepEqual(subs, want) {
		t.Fatalf("resolveSubscriptions =\n%+v\nwant\n%+v", subs, want)
	}
}

func TestResolveSubscriptionsErrors(t *testing.T) {
	cases := []struct {
		name string
		cfg  ConsumerConfig
		frag string
	}{
		{"both modes", ConsumerConfig{GroupID: "g", Subscriptions: []TopicSubscription{
			{Topic: "job", AppID: "a", AllApps: true}}}, "mutually exclusive"},
		{"bad app", ConsumerConfig{GroupID: "g", Subscriptions: []TopicSubscription{
			SubscribeApp("job", "a.b")}}, "invalid character"},
		{"empty topic", ConsumerConfig{GroupID: "g", Subscriptions: []TopicSubscription{
			SubscribeAllApps("")}}, "topic is required"},
		{"legacy and all-apps collide", ConsumerConfig{GroupID: "g", Topics: []string{"job"},
			Subscriptions: []TopicSubscription{SubscribeAllApps("job")}}, "duplicate subscription"},
		{"same app twice", ConsumerConfig{GroupID: "g", Subscriptions: []TopicSubscription{
			SubscribeApp("job", "a"), SubscribeApp("job", "a")}}, "duplicate subscription"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := resolveSubscriptions(&c.cfg)
			if err == nil || !strings.Contains(err.Error(), c.frag) {
				t.Fatalf("err = %v, want containing %q", err, c.frag)
			}
		})
	}

	// Two different apps on the same stream, plus all-apps, are distinct durables.
	// (all-apps expands to the legacy durable plus "<group>__all".)
	ok := ConsumerConfig{GroupID: "g", Subscriptions: []TopicSubscription{
		SubscribeApp("job", "a"), SubscribeApp("job", "b"), SubscribeAllApps("job")}}
	if _, err := resolveSubscriptions(&ok); err != nil {
		t.Fatalf("distinct durables rejected: %v", err)
	}
}
