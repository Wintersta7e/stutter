package report

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/gate"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// testConsumer is the consumer every block here is rendered for.
const testConsumer = "orders"

// noteConstants are the names of every constant the package declares whose name ends in Note.
func noteConstants(t *testing.T) []string {
	t.Helper()

	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list the package's files: %v", err)
	}

	var names []string

	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}

		for _, decl := range file.Decls {
			if general, isGeneral := decl.(*ast.GenDecl); isGeneral && general.Tok == token.CONST {
				names = append(names, constantsNamedNote(general)...)
			}
		}
	}

	slices.Sort(names)

	return names
}

func constantsNamedNote(decl *ast.GenDecl) []string {
	var names []string

	for _, spec := range decl.Specs {
		value, isValue := spec.(*ast.ValueSpec)
		if !isValue {
			continue
		}

		for _, name := range value.Names {
			if strings.HasSuffix(name.Name, "Note") {
				names = append(names, name.Name)
			}
		}
	}

	return names
}

// TestComposeNotesAdviseOnlyWhatTheUserCanDo: a compose user cannot declare an invariant — that is Go
// only — and gives a stubbed endpoint a firm reply with --routes. No note may advise otherwise.
func TestComposeNotesAdviseOnlyWhatTheUserCanDo(t *testing.T) {
	t.Parallel()

	notes := map[string]string{
		"guardOverrideNote":        guardOverrideNote,
		"missingReproNote":         missingReproNote,
		"missingClauseNote":        missingClauseNote,
		"mailDivergenceNote":       mailDivergenceNote,
		"unjudgeableEgressNote":    unjudgeableEgressNote,
		"opaqueDivergenceNote":     opaqueDivergenceNote,
		"readDivergenceNote":       readDivergenceNote,
		"unclassifiedNote":         unclassifiedNote,
		"stoppedNote":              stoppedNote,
		"composeMailNote":          composeMailNote,
		"composeEgressNote":        composeEgressNote,
		"composeReadNote":          composeReadNote,
		"composeUnclassifiedNote":  composeUnclassifiedNote,
		"composeGuardOverrideNote": composeGuardOverrideNote,
	}

	declared := noteConstants(t)
	for _, name := range declared {
		if _, listed := notes[name]; !listed {
			t.Errorf("note constant %s is not in this test's table, so nothing checks its compose rendering", name)
		}
	}

	t.Logf("notes=%d declared=%d", len(notes), len(declared))

	if len(declared) == 0 {
		t.Fatal("no note constant was found in the package")
	}

	for name, note := range notes {
		rendered := strings.Join(composeFindingLines(Finding{
			Consumer: testConsumer, Fault: policy.FaultDuplicate, Status: StatusWarn, Confidence: ConfidenceFirm,
			Reservations: []string{note},
		}), "\n")

		if strings.Contains(rendered, "declare an invariant") {
			t.Errorf("%s advises declaring an invariant on the compose path:\n%s", name, rendered)
		}

		if name == "guardOverrideNote" && !strings.Contains(rendered, "--routes") {
			t.Errorf("%s does not name --routes on the compose path:\n%s", name, rendered)
		}
	}

	guard := strings.Join(composeGuardDependentLines([]Finding{
		{Status: StatusWarn, Confidence: ConfidenceGuardDependent},
	}), "\n")
	if !strings.Contains(guard, "--routes") || strings.Contains(guard, "declare an invariant") {
		t.Errorf("the compose guard-dependence note must name --routes and advise no invariant:\n%s", guard)
	}
}

// TestASetupAfterCompletedRunsNeverSaysNothingWasReplayed: after runs completed, "nothing was replayed"
// is false. Before any, it is the whole message, unchanged.
func TestASetupAfterCompletedRunsNeverSaysNothingWasReplayed(t *testing.T) {
	t.Parallel()

	failed := errors.New("reset before shrink-clean: the dependency died")

	after := Report{Setup: failed, Completed: 2}
	block := strings.Join(ConsumerCheck{Name: testConsumer, Outcome: OutcomeSetup, Report: after}.lines(3), "\n")

	for name, rendered := range map[string]string{"reference": after.String(), "block": block} {
		if strings.Contains(rendered, "Nothing was replayed") {
			t.Errorf("%s: a setup error after 2 completed runs says nothing was replayed:\n%s", name, rendered)
		}

		if !strings.Contains(rendered, "2 runs") {
			t.Errorf("%s: the completed runs are not counted:\n%s", name, rendered)
		}
	}

	want := "Setup failed: " + failed.Error() +
		"\n\nNothing was replayed, so no gate ran and no findings were computed.\n"
	if got := (Report{Setup: failed}).String(); got != want {
		t.Errorf("a setup error before any run = %q, want today's text %q", got, want)
	}
}

func heldObservation() []GateCheck {
	return []GateCheck{{Name: GateObservation, Result: gate.Result{Class: gate.ClassMatch, Index: -1}}}
}

func checkedBlock(health Health) []string {
	return ConsumerCheck{
		Name: testConsumer, Outcome: OutcomeChecked, Admitted: 3,
		Report: Report{Gates: heldObservation(), Health: &health},
	}.lines(3)
}

// without is a multiset difference: the lines of got once every line of base is taken out.
func without(got, base []string) []string {
	left := slices.Clone(got)

	for _, line := range base {
		if at := slices.Index(left, line); at >= 0 {
			left = slices.Delete(left, at, at+1)
		}
	}

	slices.Sort(left)

	return left
}

// TestEveryHealthFieldRendersInTheConsumerBlock: every count the clean run carries reaches a compose
// user through the one shared renderer, and none gets a second line of its own in the block.
func TestEveryHealthFieldRendersInTheConsumerBlock(t *testing.T) {
	t.Parallel()

	// Setup and DeliveryCap render only beside something else: setup effects when nothing was observed,
	// the cap beside the messages that reached it.
	quietAlone := map[string]bool{"Setup": true, "DeliveryCap": true}
	fields := map[string]func(*Health){
		"Refusals": func(h *Health) {
			h.Refusals = []effect.Refusal{
				{
					Subject:     "$JS.API.STREAM.CREATE.ORDERS",
					Description: "stream name already in use",
					Code:        400,
					ErrCode:     10058,
				},
			}
		},
		"SilentSeqs":      func(h *Health) { h.SilentSeqs = []uint64{2} },
		"Exit":            func(h *Health) { h.Exit = replay.Exit{Code: 3, After: 2, Exited: true} },
		"Failed":          func(h *Health) { h.Failed = 1 },
		"Silent":          func(h *Health) { h.Silent = 1 },
		"Setup":           func(h *Health) { h.Setup = 2 },
		"Late":            func(h *Health) { h.Late = 1 },
		"NoResponders":    func(h *Health) { h.NoResponders = 1 },
		"FedBack":         func(h *Health) { h.FedBack = 1 },
		"Elsewhere":       func(h *Health) { h.Elsewhere = 1 },
		"ClosedAfterInfo": func(h *Health) { h.ClosedAfterInfo = 1 },
		"Owed":            func(h *Health) { h.Owed = 1 },
		"Exhausted":       func(h *Health) { h.Exhausted, h.DeliveryCap = 1, 4 },
		"DeliveryCap":     func(h *Health) { h.DeliveryCap = 4 },
	}

	summarised := []string{"Messages", "Delivered", "Effects"}

	for field := range reflect.TypeFor[Health]().Fields() {
		if _, listed := fields[field.Name]; !listed && !slices.Contains(summarised, field.Name) {
			t.Errorf("Health.%s is not in this test's table, so nothing checks that a compose user sees it", field.Name)
		}
	}

	healthy := Health{Messages: 3, Delivered: 3, Effects: 3}
	base := checkedBlock(healthy)

	for name, set := range fields {
		health := healthy
		set(&health)

		want := health.noteLines()
		if name == "SilentSeqs" {
			want = (Report{Health: &health}).notJudgedLines()
		}

		slices.Sort(want)

		got := without(checkedBlock(health), base)
		if !slices.Equal(got, want) {
			t.Errorf("Health.%s set alone added %q to the block, want exactly %q", name, got, want)
		}

		if len(got) == 0 && !quietAlone[name] {
			t.Errorf("Health.%s set alone renders nothing in the block", name)
		}
	}

	t.Logf("fields=%d", len(fields))

	if len(fields) == 0 {
		t.Fatal("no Health field was checked")
	}
}

func TestNotJudgedMessagesAreNamedWithTheirFiles(t *testing.T) {
	t.Parallel()

	health := Health{Messages: 6, Delivered: 6, Effects: 4, Silent: 2, SilentSeqs: []uint64{2, 5}}
	files := map[uint64]string{2: "2.orders.created.json", 5: "5.orders.shipped.json"}
	block := ConsumerCheck{
		Name: testConsumer, Outcome: OutcomeChecked, Admitted: 6,
		Report: Report{Gates: heldObservation(), Health: &health, Files: files},
	}.lines(6)

	var named []string

	for _, line := range block {
		if strings.HasPrefix(line, notJudgedLabel) {
			named = append(named, line)
		}
	}

	if len(named) != 1 || !strings.Contains(named[0], "#2 (2.orders.created.json)") ||
		!strings.Contains(named[0], "#5 (5.orders.shipped.json)") {
		t.Errorf("NOT JUDGED lines = %q, want one naming #2 and #5 with their files", named)
	}

	silent := Health{Messages: 2, Delivered: 2, Silent: 2, SilentSeqs: []uint64{2, 5}}
	allSilent := ConsumerCheck{Name: testConsumer, Outcome: OutcomeChecked, Admitted: 2, Report: Report{
		Gates:    []GateCheck{{Name: GateObservation, Result: gate.Observed(nil)}},
		Health:   &silent,
		Coverage: []Coverage{{Fault: policy.FaultDuplicate, Pairs: 2, Attempted: 2, Legal: true}},
	}}

	if got := allSilent.Bucket(); got == BucketPass {
		t.Errorf("a consumer whose every message was silent lands in %q", got)
	}
}

var coverageCounts = regexp.MustCompile(`(\d+) pairs? — (\d+) attempted, (\d+) unexpressed, (\d+) cut`)

func coverageLinesOf(block []string) []string {
	var lines []string

	for _, line := range block {
		if strings.HasPrefix(line, detailIndent+coverageLabel) {
			lines = append(lines, line)
		}
	}

	return lines
}

// TestCoverageLinesHoldTheInvariant: a legal fault's line adds up to its pairs; an illegal one names the
// clause refusing it; a consumer licensing nothing says so; gate mode tries nothing and says nothing.
func TestCoverageLinesHoldTheInvariant(t *testing.T) {
	t.Parallel()

	const refusing = "MaxAckPending: 1 — one message in flight at a time"

	coverage := []Coverage{
		{Fault: policy.FaultDuplicate, Pairs: 3, Attempted: 3, Legal: true},
		{Fault: policy.FaultCrashBeforeAck, Pairs: 3, Attempted: 1, CutByBudget: 2, Legal: true},
		{Fault: policy.FaultDelay, Pairs: 3, Unexpressed: 3, Legal: true},
		{Fault: policy.FaultReorder, Clause: refusing},
	}

	health := Health{Messages: 3, Delivered: 3, Effects: 3}
	checked := ConsumerCheck{Name: testConsumer, Outcome: OutcomeChecked, Admitted: 3, Report: Report{
		Gates: heldObservation(), Health: &health, Coverage: coverage,
	}}

	lines := coverageLinesOf(checked.lines(3))
	if len(lines) != len(coverage) {
		t.Fatalf("%d coverage lines, want %d:\n%s", len(lines), len(coverage), strings.Join(lines, "\n"))
	}

	for index, line := range coverage {
		rendered := lines[index]
		if !strings.Contains(rendered, describeFault(line.Fault)) {
			t.Errorf("coverage line %d = %q, want %s in the check's order", index, rendered, describeFault(line.Fault))
		}

		if !line.Legal {
			if !strings.Contains(rendered, refusing) {
				t.Errorf("an illegal fault's line %q does not name its clause", rendered)
			}

			continue
		}

		counts := coverageCounts.FindStringSubmatch(rendered)
		if counts == nil {
			t.Fatalf("a legal fault's line %q does not print its four counts", rendered)
		}

		numbers := make([]int, 0, len(counts)-1)
		for _, count := range counts[1:] {
			number, err := strconv.Atoi(count)
			if err != nil {
				t.Fatalf("%q: count %q: %v", rendered, count, err)
			}

			numbers = append(numbers, number)
		}

		if numbers[1]+numbers[2]+numbers[3] != numbers[0] {
			t.Errorf("%q: the counts do not add up to the pairs", rendered)
		}
	}

	illegal := checked
	illegal.Report.Coverage = []Coverage{{Fault: policy.FaultDuplicate, Clause: "AckPolicy: none"}}

	if got := coverageLinesOf(illegal.lines(3)); !strings.Contains(strings.Join(got, "\n"), noLegalFault) {
		t.Errorf("a consumer licensing no fault renders %q, want the 0-legal line", got)
	}

	gateMode := checked
	gateMode.Report.GatesOnly = true

	if got := coverageLinesOf(gateMode.lines(3)); len(got) != 0 {
		t.Errorf("gate mode renders coverage %q, want none", got)
	}
}
