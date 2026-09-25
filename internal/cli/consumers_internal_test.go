package cli

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/provision"
	httpproxy "github.com/Wintersta7e/stutter/internal/proxy/http"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
)

// Names several fixtures share.
const (
	subjectCreated    = "orders.created"
	consumerControl   = "control"
	consumerPlanted   = "planted"
	consumerElsewhere = "AUDIT/archiver"
)

// sixMessages is a corpus over two subject families, one file per message.
func sixMessages() corpus.Loaded {
	subjects := []string{
		subjectCreated, "orders.shipped", "payments.taken", subjectCreated, "orders.eu.created", "audit.log",
	}

	loaded := corpus.Loaded{}

	for index, subject := range subjects {
		seq := uint64(index + 1)
		loaded.Messages = append(loaded.Messages, corpus.Message{Subject: subject, Payload: []byte("{}"), Seq: seq})
		loaded.Files = append(loaded.Files, fmt.Sprintf("%d.%s.json", seq, subject))
	}

	return loaded
}

// licensing is a configuration every fault the check tries is legal under.
func licensing(filters ...string) policy.Config {
	return policy.Config{
		AckMode: policy.AckExplicit, FilterSubjects: filters, AckWait: time.Second, MaxDeliver: -1, MaxAckPending: 10,
	}
}

// fakeSandbox is a consumer check's session that never diverges: every run does the same work for every
// message it is handed.
type fakeSandbox struct {
	afterCheck func()
	tally      []httpproxy.HostTally
	holds      []harness.FillHold
	seqs       []uint64
	runs       int
}

func (*fakeSandbox) Reset(context.Context) error { return nil }

func (f *fakeSandbox) Run(ctx context.Context, _ string, _ replay.Mutation, retain []uint64) (replay.Result, error) {
	if err := ctx.Err(); err != nil {
		return replay.Result{}, err
	}

	f.runs++

	scope := f.seqs
	if len(retain) > 0 {
		scope = retain
	}

	effects := make([]effect.Effect, 0, len(scope))
	for _, seq := range scope {
		effects = append(effects, effect.Effect{
			Kind: effect.KindPostgres, Canonical: fmt.Sprintf("UPDATE stock WHERE id = %d", seq), MessageSeq: seq,
		})
	}

	return replay.Result{Effects: effects, Delivered: len(scope), Clause: "AckPolicy: explicit"}, nil
}

func (f *fakeSandbox) Tally() []httpproxy.HostTally { return f.tally }

func (*fakeSandbox) Timings() harness.Timings {
	return harness.Timings{Startup: time.Minute, Quiesce: time.Second, Drain: 5 * time.Second}
}

func (f *fakeSandbox) Holds() []harness.FillHold {
	if f.afterCheck != nil {
		f.afterCheck()
	}

	return f.holds
}

// fakeCheck is a compose check whose discovery found consumers, over sixMessages, with sandboxes that
// never diverge. built counts the sandboxes built, per consumer.
func fakeCheck(found harness.Discovery, run composeRun) (*composeCheck, map[string]*fakeSandbox) {
	var stderr strings.Builder

	check := newComposeCheck(run, newLockedWriter(&stderr))
	check.found = found
	check.loaded = sixMessages()
	check.logHold = func(provision.Hold) error { return nil }

	built := map[string]*fakeSandbox{}
	check.newSandbox = func(plan consumerPlan) (sandbox, error) {
		fake := &fakeSandbox{seqs: plan.admitted}
		built[plan.name] = fake

		return fake, nil
	}

	return check, built
}

func byName(checks []report.ConsumerCheck, name string) report.ConsumerCheck {
	index := slices.IndexFunc(checks, func(c report.ConsumerCheck) bool { return c.Name == name })
	if index < 0 {
		return report.ConsumerCheck{}
	}

	return checks[index]
}

// TestAdmissionUsesTheBusMatcher: a consumer is checked over exactly the corpus messages its filter
// subjects admit, wildcards read as the bus reads them.
func TestAdmissionUsesTheBusMatcher(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		filters []string
		want    []uint64
	}{
		{"single filter", []string{subjectCreated}, []uint64{1, 4}},
		{"multi filter", []string{"orders.shipped", "audit.log"}, []uint64{2, 6}},
		{"one-token wildcard", []string{"orders.*"}, []uint64{1, 2, 4}},
		{"tail wildcard", []string{"orders.>"}, []uint64{1, 2, 4, 5}},
	}

	for _, testCase := range cases {
		found := harness.Discovery{Consumers: []harness.Found{{Name: "c", Policy: licensing(testCase.filters...)}}}

		plans, err := planConsumers(found, sixMessages().Messages, nil)
		if err != nil {
			t.Fatalf("%s: plan: %v", testCase.name, err)
		}

		check, _ := fakeCheck(found, composeRun{})
		if got := check.checkOptions(plans[0]).Messages; !slices.Equal(got, testCase.want) {
			t.Errorf("%s: Options.Messages = %v, want %v", testCase.name, got, testCase.want)
		}
	}

	t.Logf("cases=%d", len(cases))
}

// TestConsumerNarrowsTheCheck: --consumer checks exactly the consumers it names; the rest are named as
// not selected, never run.
func TestConsumerNarrowsTheCheck(t *testing.T) {
	t.Parallel()

	found := harness.Discovery{Consumers: []harness.Found{
		{Name: consumerControl, Policy: licensing("orders.>")}, {Name: consumerPlanted, Policy: licensing("orders.>")},
	}}
	check, built := fakeCheck(found, composeRun{consumers: []string{consumerControl}, maxRuns: 1})

	checks, unnamed, err := check.consumerChecks(t.Context())
	if err != nil || unnamed != nil {
		t.Fatalf("consumerChecks = (%v, %v)", unnamed, err)
	}

	if got := byName(checks, consumerPlanted); got.Outcome != report.OutcomeNotSelected {
		t.Errorf("the consumer --consumer left out is %q, want not-selected", got.Outcome)
	}

	if _, ran := built[consumerPlanted]; ran {
		t.Error("a sandbox was built for a consumer --consumer left out")
	}

	if got := byName(checks, consumerControl); got.Outcome != report.OutcomeChecked || got.Report.Health == nil {
		t.Errorf("the named consumer is %q with health %v, want checked", got.Outcome, got.Report.Health)
	}
}

// TestAConsumerNamedButNotDiscoveredNamesEveryDiscoveredOne: a --consumer typo is refused, listing what
// could have been meant.
func TestAConsumerNamedButNotDiscoveredNamesEveryDiscoveredOne(t *testing.T) {
	t.Parallel()

	found := harness.Discovery{
		Consumers: []harness.Found{
			{Name: consumerControl},
			{Name: consumerPlanted},
		},
		Elsewhere: []string{consumerElsewhere},
	}

	_, err := planConsumers(found, sixMessages().Messages, []string{"nosuch"})
	if !errors.Is(err, errConsumerNotDiscovered) {
		t.Fatalf("plan = %v, want %v", err, errConsumerNotDiscovered)
	}

	for _, name := range []string{"nosuch", consumerControl, consumerPlanted, consumerElsewhere} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("the refusal %q does not name %s", err, name)
		}
	}
}

// TestAConsumerAdmittingNothingIsNeverRun: a consumer no corpus message reaches cannot be checked, and
// says so rather than passing.
func TestAConsumerAdmittingNothingIsNeverRun(t *testing.T) {
	t.Parallel()

	found := harness.Discovery{Consumers: []harness.Found{
		{Name: "idle", Policy: licensing("inventory.>")}, {Name: "orders", Policy: licensing("orders.>")},
	}}
	check, built := fakeCheck(found, composeRun{maxRuns: 1})

	checks, _, err := check.consumerChecks(t.Context())
	if err != nil {
		t.Fatalf("consumerChecks: %v", err)
	}

	idle := byName(checks, "idle")
	if idle.Outcome != report.OutcomeNotCovered || idle.Reason == "" || idle.Admitted != 0 {
		t.Errorf("idle = %q (%q, %d admitted), want not-covered with a reason", idle.Outcome, idle.Reason,
			idle.Admitted)
	}

	if fake, ran := built["idle"]; ran && fake.runs > 0 {
		t.Errorf("the consumer admitting nothing ran %d times", fake.runs)
	}
}

// TestUnstableElsewhereAndExcludedConsumersAreNotChecked: each is named with why.
func TestUnstableElsewhereAndExcludedConsumersAreNotChecked(t *testing.T) {
	t.Parallel()

	found := harness.Discovery{
		Consumers: []harness.Found{
			{Name: "ephemeral", Policy: licensing("orders.>"), Unstable: true},
			{Name: "ordered", Policy: licensing("orders.>"), Excluded: "an ordered consumer cannot be a target"},
		},
		Elsewhere: []string{consumerElsewhere},
	}

	plans, err := planConsumers(found, sixMessages().Messages, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	want := map[string]report.Outcome{
		consumerElsewhere: report.OutcomeNotCovered,
		"ephemeral":       report.OutcomeNotCovered,
		"ordered":         report.OutcomeSetup,
	}

	for _, plan := range plans {
		if plan.outcome != want[plan.name] || plan.reason == "" {
			t.Errorf("%s = %q (%q), want %q with a reason", plan.name, plan.outcome, plan.reason, want[plan.name])
		}
	}

	if len(plans) != len(want) {
		t.Errorf("%d plans, want %d", len(plans), len(want))
	}
}

// TestNoConsumerAnywhereRunsOneUnnamedCheck: a service with no consumer still gets one check, never
// counted as a discovered consumer, and its reason names what the bus refused.
func TestNoConsumerAnywhereRunsOneUnnamedCheck(t *testing.T) {
	t.Parallel()

	found := harness.Discovery{
		Refusals: []effect.Refusal{
			{Subject: "$JS.API.STREAM.CREATE.ORDERS", ErrCode: 10058, Description: "in use"},
		},
		ClosedAfterInfo: 2,
	}
	check, built := fakeCheck(found, composeRun{maxRuns: 1})

	checks, unnamed, err := check.consumerChecks(t.Context())
	if err != nil {
		t.Fatalf("consumerChecks: %v", err)
	}

	if len(checks) != 0 || unnamed == nil {
		t.Fatalf("consumerChecks = %d checks and unnamed %v, want only the unnamed check", len(checks), unnamed)
	}

	for _, want := range []string{"$JS.API.STREAM.CREATE.ORDERS", "10058", "2 bus connections"} {
		if !strings.Contains(unnamed.Reason, want) {
			t.Errorf("the unnamed check's reason %q does not name %q", unnamed.Reason, want)
		}
	}

	if fake := built[""]; fake == nil || fake.runs == 0 {
		t.Error("the unnamed check never ran")
	}
}

// TestConsumersOnlyElsewhereStopTheCheck: discovery's own refusal is whole-check, decided once, and
// names where the service consumes instead.
func TestConsumersOnlyElsewhereStopTheCheck(t *testing.T) {
	t.Parallel()

	check := newComposeCheck(composeRun{}, newLockedWriter(&strings.Builder{}))
	check.discover = func(context.Context) (harness.Discovery, error) {
		return harness.Discovery{Elsewhere: []string{consumerElsewhere}},
			fmt.Errorf("%w: AUDIT/archiver", harness.ErrConsumesElsewhere)
	}

	err := check.discovery(t.Context())
	if !errors.Is(err, harness.ErrConsumesElsewhere) || !strings.Contains(err.Error(), consumerElsewhere) {
		t.Errorf("discovery = %v, want %v naming AUDIT/archiver", err, harness.ErrConsumesElsewhere)
	}
}

// TestEveryConsumerGetsTheLoadedCorpus: every consumer check replays the corpus as loaded from the
// directory, never what a stream holds after another check ran, and is checked over its own messages.
func TestEveryConsumerGetsTheLoadedCorpus(t *testing.T) {
	t.Parallel()

	found := harness.Discovery{Consumers: []harness.Found{
		{Name: "created", Policy: licensing(subjectCreated)}, {Name: "shipped", Policy: licensing("orders.shipped")},
	}}
	check, _ := fakeCheck(found, composeRun{maxRuns: 1})
	loaded := sixMessages()

	plans, err := planConsumers(found, loaded.Messages, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, plan := range plans {
		config := sandboxConfig(harness.Config{}, plan, check.run, loaded.Messages)
		if !slices.EqualFunc(
			config.Recorded,
			loaded.Messages,
			func(a, b corpus.Message) bool { return a.Seq == b.Seq },
		) {
			t.Errorf(
				"%s: Recorded = %d messages, want the %d loaded",
				plan.name,
				len(config.Recorded),
				len(loaded.Messages),
			)
		}

		if config.Consumer != plan.name || config.Policy.FilterSubjects[0] != plan.found.Policy.FilterSubjects[0] {
			t.Errorf("%s: the sandbox names %q under %v", plan.name, config.Consumer, config.Policy.FilterSubjects)
		}
	}

	checks, _, err := check.consumerChecks(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	for _, consumer := range checks {
		if consumer.Report.Health == nil || consumer.Report.Health.Messages != consumer.Admitted {
			t.Errorf("%s: the clean run was given %v messages, want the %d admitted", consumer.Name,
				consumer.Report.Health, consumer.Admitted)
		}
	}
}

// TestEachConsumerSpendsItsOwnBudget: --max-runs is per consumer check, so the second consumer is
// checked as thoroughly as the first.
func TestEachConsumerSpendsItsOwnBudget(t *testing.T) {
	t.Parallel()

	found := harness.Discovery{Consumers: []harness.Found{
		{Name: "a", Policy: licensing("orders.>")}, {Name: "b", Policy: licensing("orders.>")},
	}}
	check, _ := fakeCheck(found, composeRun{maxRuns: 1})

	checks, _, err := check.consumerChecks(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	for _, consumer := range checks {
		attempted := 0
		for _, line := range consumer.Report.Coverage {
			attempted += line.Attempted
		}

		t.Logf("%s attempted=%d", consumer.Name, attempted)

		if attempted != 1 {
			t.Errorf("consumer %s attempted %d faults under --max-runs 1, want 1", consumer.Name, attempted)
		}
	}
}

// TestAnInterruptEndsTheRestInSetup: a consumer check that completed keeps its verdict; every one after
// the interrupt ends in setup, and none passes.
func TestAnInterruptEndsTheRestInSetup(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	found := harness.Discovery{Consumers: []harness.Found{
		{Name: "a", Policy: licensing("orders.>")},
		{Name: "b", Policy: licensing("orders.>")},
		{Name: "c", Policy: licensing("orders.>")},
	}}
	check, built := fakeCheck(found, composeRun{maxRuns: 1})

	newSandbox := check.newSandbox
	check.newSandbox = func(plan consumerPlan) (sandbox, error) {
		made, err := newSandbox(plan)
		if plan.name == "a" {
			built["a"].afterCheck = cancel
		}

		return made, err
	}

	checks, _, err := check.consumerChecks(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if got := byName(checks, "a").Bucket(); got != report.BucketPass {
		t.Errorf("the completed consumer check is %q, want its own verdict, pass", got)
	}

	for _, name := range []string{"b", "c"} {
		consumer := byName(checks, name)
		if consumer.Bucket() != report.BucketSetup || !strings.Contains(consumer.Reason, "interrupt") {
			t.Errorf("%s after the interrupt = %q (%q), want setup naming the interrupt", name, consumer.Bucket(),
				consumer.Reason)
		}
	}
}

// TestADrainBelowAnyConsumersFloorRefusesTheCheck: every selected consumer's sandbox is built before the
// first check, so a drain one of them cannot use stops the check before any run.
func TestADrainBelowAnyConsumersFloorRefusesTheCheck(t *testing.T) {
	t.Parallel()

	quick := licensing("orders.>")
	slow := licensing("orders.>")
	slow.AckWait = 10 * time.Second

	found := harness.Discovery{Consumers: []harness.Found{{Name: "a", Policy: quick}, {Name: "b", Policy: slow}}}
	drain := harness.DrainFloor(quick, 0) + time.Second
	check, _ := fakeCheck(found, composeRun{drain: drain, maxRuns: 1})

	runs := 0
	check.newSandbox = func(plan consumerPlan) (sandbox, error) {
		start := func(context.Context, harness.Addresses) (harness.Consumer, error) { return nil, errRunFailed }
		if _, err := harness.New(
			sandboxConfig(harness.Config{Start: start}, plan, check.run, check.loaded.Messages),
		); err != nil {
			return nil, err
		}

		return &fakeSandbox{afterCheck: func() { runs++ }}, nil
	}

	_, _, err := check.consumerChecks(t.Context())
	if !errors.Is(err, harness.ErrDrainBelowFloor) {
		t.Fatalf("consumerChecks = %v, want %v", err, harness.ErrDrainBelowFloor)
	}

	floor := harness.DrainFloor(slow, 0).String()
	if !strings.Contains(err.Error(), "b") || !strings.Contains(err.Error(), floor) {
		t.Errorf("the refusal %q does not name consumer b and its floor %s", err, floor)
	}

	if runs != 0 {
		t.Errorf("%d consumer checks ran before the refusal", runs)
	}
}

// TestTalliesSumAcrossConsumerChecks: an external host answered in two consumer checks is one header
// row with both checks' calls.
func TestTalliesSumAcrossConsumerChecks(t *testing.T) {
	t.Parallel()

	found := harness.Discovery{Consumers: []harness.Found{
		{Name: "a", Policy: licensing("orders.>")}, {Name: "b", Policy: licensing("orders.>")},
	}}
	check, _ := fakeCheck(found, composeRun{maxRuns: 1})

	newSandbox := check.newSandbox
	check.newSandbox = func(plan consumerPlan) (sandbox, error) {
		made, err := newSandbox(plan)

		fake, isFake := made.(*fakeSandbox)
		if !isFake {
			return nil, errRunFailed
		}

		fake.tally = []httpproxy.HostTally{{Host: "payments.example.test", Calls: 2}}

		return made, err
	}

	if _, _, err := check.consumerChecks(t.Context()); err != nil {
		t.Fatal(err)
	}

	hosts := check.header.Hosts
	if hosts == nil || len(*hosts) != 1 || (*hosts)[0].Calls != 4 {
		t.Errorf("header hosts = %v, want one host with 4 calls", hosts)
	}

	if check.header.Timings == nil {
		t.Error("the header carries no timings from the consumer checks")
	}
}

// TestAFillCollisionNamesBothFiles: two corpus messages the bus took for one are named by the files a
// user opens, never by sequences alone.
func TestAFillCollisionNamesBothFiles(t *testing.T) {
	t.Parallel()

	check, _ := fakeCheck(harness.Discovery{}, composeRun{})
	err := fmt.Errorf("run clean-1: %w", &corpus.FillError{Seq: 2, Other: 5})

	reason := check.setupReason(err)
	for _, file := range []string{"2.orders.shipped.json", "5.orders.eu.created.json"} {
		if !strings.Contains(reason, file) {
			t.Errorf("the reason %q does not name %s", reason, file)
		}
	}
}

// TestEveryHeldRunIsLogged: each run's hold goes to the invocation log, whole, and nowhere else.
func TestEveryHeldRunIsLogged(t *testing.T) {
	t.Parallel()

	var logged []provision.Hold

	holds := []harness.FillHold{
		{Term: "deadline", Length: 3 * time.Millisecond, Bound: 100 * time.Millisecond, Messages: 2, Bytes: 40},
		{Term: "expires", Length: 4 * time.Millisecond, Bound: 50 * time.Millisecond, Messages: 1, Bytes: 12},
	}

	err := holdLines(holds, func(hold provision.Hold) error {
		logged = append(logged, hold)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("lines=%d", len(logged))

	if len(logged) != len(holds) {
		t.Fatalf("%d hold lines, want %d", len(logged), len(holds))
	}

	for index, hold := range holds {
		got := logged[index]
		if got.Term != hold.Term || got.Length != hold.Length || got.Bound != hold.Bound ||
			got.Messages != hold.Messages || got.Bytes != int64(hold.Bytes) {
			t.Errorf("hold %d logged as %+v, want %+v", index, got, hold)
		}
	}
}
