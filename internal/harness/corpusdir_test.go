package harness_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/report"
)

// userStream is the stream a user's service consumes from, named as --stream would name it.
const userStream = "ORDERS_P9"

// assembled is a corpus directory taken through the steps a compose check runs before its first
// run: the directory read, the bus opened bound to the user's stream, B0 checkpointed, the stream's
// owner settled and the stream made, B1 checkpointed.
type assembled struct {
	store  *corpus.Corpus
	b1     *corpus.Checkpoint
	loaded corpus.Loaded
}

// assemble lays a corpus directory down from files and takes it through the pre-run steps.
func assemble(t *testing.T, files map[string]string) assembled {
	t.Helper()

	dir := t.TempDir()

	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
	}

	loaded, err := corpus.LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir() error = %v", err)
	}

	private := filepath.Join(t.TempDir(), "bus")
	if mkdirErr := os.Mkdir(private, 0o700); mkdirErr != nil {
		t.Fatalf("Mkdir() error = %v", mkdirErr)
	}

	store, err := corpus.Open(t.Context(), filepath.Join(private, "store"), userStream)
	if err != nil {
		t.Fatalf("corpus.Open() error = %v", err)
	}

	t.Cleanup(store.Close)

	if _, b0Err := store.Checkpoint(t.Context(), filepath.Join(private, "B0")); b0Err != nil {
		t.Fatalf("Checkpoint(B0) error = %v", b0Err)
	}

	// No job created the stream, and no service start is made in-process: the two surveys agree.
	before := surveyed(t, store)
	after := surveyed(t, store)

	if establishErr := store.Establish(t.Context(), before, after, loaded.Messages); establishErr != nil {
		t.Fatalf("Establish() error = %v", establishErr)
	}

	b1, err := store.Checkpoint(t.Context(), filepath.Join(private, "B1"))
	if err != nil {
		t.Fatalf("Checkpoint(B1) error = %v", err)
	}

	return assembled{store: store, b1: &b1, loaded: loaded}
}

func surveyed(t *testing.T, store *corpus.Corpus) corpus.Survey {
	t.Helper()

	survey, err := store.Survey(t.Context())
	if err != nil {
		t.Fatalf("Survey() error = %v", err)
	}

	return survey
}

// sandboxFor builds a sandbox around a service consuming the user's stream, handed the assembled
// corpus and its B1.
func sandboxFor(t *testing.T, config policy.Config, behaviour quirks, built assembled) *harness.Sandbox {
	t.Helper()

	behaviour.stream = userStream

	sandbox, _ := quirkySandbox(t, config, behaviour, func(settings *harness.Config) {
		settings.Corpus = built.store
		settings.Baseline = built.b1
		settings.Recorded = built.loaded.Messages
	})

	return sandbox
}

// ranks are the recorded identities of a loaded corpus: its ranks, 1 to N.
func ranks(loaded corpus.Loaded) []uint64 {
	seqs := make([]uint64, 0, len(loaded.Messages))
	for _, message := range loaded.Messages {
		seqs = append(seqs, message.Seq)
	}

	return seqs
}

// order is one order message's JSON.
func order(id string) string {
	return string(orderPayload(id, "WIDGET-DIR"))
}

// TestACorpusDirectoryDrivesAnObservedCheck: every producer the compose check will call composes —
// the directory reader, the bus bound to a stream of the user's naming, the checkpoints, stream
// ownership, the restore before every run and the hold — before that check exists.
func TestACorpusDirectoryDrivesAnObservedCheck(t *testing.T) {
	t.Parallel()

	built := assemble(t, map[string]string{
		"1.orders.created.json":    order("ORD-DIR-1"),
		"1.orders.created.headers": idempotencyKey + ": key-1\n",
		"2.orders.priority.json":   order("ORD-DIR-2"),
		"3.orders.created.json":    order("ORD-DIR-3"),
		".notes":                   "not a message",
	})

	if built.loaded.Sidecars != 1 || built.loaded.Skipped != 1 {
		t.Errorf("Sidecars = %d, Skipped = %d, want 1 and 1", built.loaded.Sidecars, built.loaded.Skipped)
	}

	config := observedConfig()
	config.FilterSubjects = nil
	sandbox := sandboxFor(t, config, quirks{}, built)
	session := &counting{inner: sandbox}

	result, err := check.Run(t.Context(), session, check.Options{
		Messages: ranks(built.loaded),
		Consumer: observedConsumer,
		Config:   config,
		MaxRuns:  1,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if len(result.Gates) == 0 || len(result.Violations()) != 0 {
		t.Fatalf("the gates did not hold:\n%s", result)
	}

	if result.Health == nil || result.Health.Messages != len(built.loaded.Messages) {
		t.Errorf("Health = %+v, want %d messages", result.Health, len(built.loaded.Messages))
	}

	if holds := sandbox.Holds(); session.runs == 0 || len(holds) != session.runs {
		t.Errorf("%d holds for %d runs, want one per run", len(holds), session.runs)
	}

	want := []string{"orders.created", "orders.priority"}
	if got := streamSubjects(t, built.store); !slices.Equal(got, want) {
		t.Errorf("stream subjects = %q, want the distinct corpus subjects %q", got, want)
	}
}

// streamSubjects reads the user's stream's subjects back from the bus.
func streamSubjects(t *testing.T, store *corpus.Corpus) []string {
	t.Helper()

	connection, err := nats.Connect(store.URL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}

	defer connection.Close()

	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	stream, err := js.Stream(t.Context(), userStream)
	if err != nil {
		t.Fatalf("Stream(%s) error = %v", userStream, err)
	}

	return stream.CachedInfo().Config.Subjects
}

// TestAHeaderKeyedGuardHoldsUnderDuplicate: a handler whose dedupe guard keys on a header the corpus
// supplies is correct only if the header reaches it. Dropped at Fill, the guard has nothing to key on,
// the handler repeats its work under duplicate delivery, and a correct handler is reported diverging.
func TestAHeaderKeyedGuardHoldsUnderDuplicate(t *testing.T) {
	t.Parallel()

	files := make(map[string]string)

	for at := 1; at <= 3; at++ {
		files[fmt.Sprintf("%d.orders.created.json", at)] = order(fmt.Sprintf("ORD-KEY-%d", at))
		files[fmt.Sprintf("%d.orders.created.headers", at)] = fmt.Sprintf("%s: key-%d\n", idempotencyKey, at)
	}

	built := assemble(t, files)

	config := observedConfig()
	config.FilterSubjects = nil

	if !config.Permits(policy.FaultDuplicate).Permitted {
		t.Fatal("duplicate delivery is not legal under this configuration, so the guard is never tested")
	}

	session := &counting{inner: sandboxFor(t, config, quirks{headerGuard: true}, built)}

	result, err := check.Run(t.Context(), session, check.Options{
		Messages: ranks(built.loaded),
		Consumer: observedConsumer,
		Config:   config,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	if session.faults == 0 {
		t.Fatal("no fault was injected, so the guard was never tested")
	}

	if len(result.Violations()) != 0 {
		t.Fatalf("the gates did not hold:\n%s", result)
	}

	for _, finding := range result.Findings {
		t.Errorf("%s under %s: the header-keyed guard diverged\n%s", finding.Status, finding.Fault, result)
	}

	if got := result.ExitCode(); got != report.ExitPass {
		t.Errorf("ExitCode() = %d, want a pass", got)
	}
}
