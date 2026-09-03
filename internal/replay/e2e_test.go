package replay_test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/gate"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// envPostgres names a reachable Postgres, e.g.
// postgres://stutter:stutter@127.0.0.1:5432/stutter?sslmode=disable
//
// Without it these tests skip rather than fail: they exercise the real wire protocol against a real
// server, and a stub database would prove nothing about either.
const envPostgres = "STUTTER_TEST_POSTGRES"

const (
	orderedQty = 3
	hashKeyLen = 32
	// testAckWait is short so a delay fault can outlast it without the suite taking minutes.
	testAckWait = 2 * time.Second
)

// serialConfig mirrors an ordered consumer: one message outstanding, explicit acks, capped
// redelivery. Reordering and concurrency are forbidden under it, which is the point.
func serialConfig() policy.Config {
	return policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{"corpus.>"},
		AckWait:        testAckWait,
		MaxDeliver:     8,
		MaxAckPending:  1,
	}
}

type harness struct {
	store     *corpus.Corpus
	runner    *replay.Runner
	directDSN string
	upstream  string
	sku       string
	orderSeq  uint64
}

func orderPayload(orderID, sku string, qty int) []byte {
	return fmt.Appendf(nil, `{"order_id":%q,"sku":%q,"qty":%d}`, orderID, sku, qty)
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	return newHarnessWith(t, serialConfig())
}

func newHarnessWith(t *testing.T, config policy.Config) *harness {
	t.Helper()

	directDSN := os.Getenv(envPostgres)
	if directDSN == "" {
		t.Skipf("%s is not set; skipping the test that needs a real database", envPostgres)
	}

	parsed, err := url.Parse(directDSN)
	if err != nil {
		t.Fatalf("parse %s: %v", envPostgres, err)
	}

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	// A SKU per test: these run in parallel against one database, and a shared fixture reset is a
	// race that surfaces as a divergence the service never caused.
	sku := "WIDGET-" + t.Name()

	h := &harness{
		store:     store,
		runner:    replay.NewRunner(store, key, config, replay.Options{Quiesce: 50 * time.Millisecond}),
		directDSN: directDSN,
		upstream:  parsed.Host,
		sku:       sku,
	}
	h.orderSeq = h.publish(t, toy.SubjectOrderCreated, orderPayload("ORD-99001", sku, orderedQty))

	return h
}

func (h *harness) publish(t *testing.T, subject string, payload []byte) uint64 {
	t.Helper()

	seq, err := h.store.Publish(t.Context(), subject, payload)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	return seq
}

// proxiedDSN points the consumer at the proxy instead of the database, with TLS disabled: an
// encrypted connection is unreadable, and the proxy refuses it rather than reporting a handler as
// having produced no effects.
func (h *harness) proxiedDSN(t *testing.T, addr string) string {
	t.Helper()

	parsed, err := url.Parse(h.directDSN)
	if err != nil {
		t.Fatalf("parse %s: %v", envPostgres, err)
	}

	parsed.Host = addr

	query := parsed.Query()
	query.Set("sslmode", "disable")
	parsed.RawQuery = query.Encode()

	return parsed.String()
}

// run resets the fixture, replays the corpus once under one mutation, and returns what happened.
func (h *harness) run(t *testing.T, name string, mutation replay.Mutation) replay.Result {
	t.Helper()

	result, err := h.tryRun(t, name, mutation)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.Failed > 0 {
		t.Fatalf("run health: %d of %d deliveries failed; a rejected corpus proves nothing",
			result.Failed, result.Delivered)
	}

	return result
}

func (h *harness) tryRun(t *testing.T, name string, mutation replay.Mutation) (replay.Result, error) {
	t.Helper()

	if err := toy.Setup(t.Context(), h.directDSN, h.sku); err != nil {
		t.Fatalf("toy.Setup() error = %v", err)
	}

	recorder := h.runner.NewRecorder()

	proxy, err := pg.Listen(t.Context(), "127.0.0.1:0", h.upstream, recorder)
	if err != nil {
		t.Fatalf("pg.Listen() error = %v", err)
	}

	// Surfaced rather than discarded: a proxy that died mid-run would otherwise present as a
	// handler that simply stopped producing effects.
	served := make(chan error, 1)
	go func() { served <- proxy.Serve(t.Context()) }()

	defer func() {
		if closeErr := proxy.Close(); closeErr != nil {
			t.Errorf("proxy.Close() error = %v", closeErr)
		}

		if serveErr := <-served; serveErr != nil {
			t.Errorf("proxy.Serve() error = %v", serveErr)
		}
	}()

	consumer, err := toy.Connect(t.Context(), h.proxiedDSN(t, proxy.Addr()))
	if err != nil {
		t.Fatalf("toy.Connect() error = %v", err)
	}

	defer consumer.Close(t.Context())

	// The guard talks to the bus directly rather than through the proxy: NATS egress is not yet
	// observed, so wiring it through would capture nothing and only add a hop.
	guard, err := toy.NewGuard(t.Context(), h.store.URL())
	if err != nil {
		t.Fatalf("toy.NewGuard() error = %v", err)
	}

	defer guard.Close()

	// Reset alongside the database: bus-side state left over from a previous run corrupts the
	// comparison exactly as leftover rows would.
	if resetErr := guard.Reset(t.Context()); resetErr != nil {
		t.Fatalf("guard.Reset() error = %v", resetErr)
	}

	consumer.UseGuard(guard)

	result, err := h.runner.Run(t.Context(), name, mutation, consumer.Handle, recorder)
	if err != nil {
		return result, fmt.Errorf("run %q: %w", name, err)
	}

	return result, nil
}

func (h *harness) qty(t *testing.T) int {
	t.Helper()

	got, err := toy.Qty(t.Context(), h.directDSN, h.sku)
	if err != nil {
		t.Fatalf("toy.Qty() error = %v", err)
	}

	return got
}

// TestDeterminismGateHolds is build-order step 1. Until two unmutated runs agree byte for byte,
// every mutation result is noise and nothing downstream is worth building.
func TestDeterminismGateHolds(t *testing.T) {
	t.Parallel()

	h := newHarness(t)

	first := h.run(t, "clean-a", replay.Clean{})
	second := h.run(t, "clean-b", replay.Clean{})

	if len(first.Effects) == 0 {
		t.Fatal("the clean run produced no effects; the proxy observed nothing")
	}

	if first.Late > 0 {
		t.Errorf("%d effects arrived after their attribution window closed", first.Late)
	}

	if result := gate.NewComparer().Compare(first.Effects, second.Effects); !result.OK() {
		t.Errorf("determinism gate failed: %s", result.Describe())
	}
}

// TestDuplicateDeliveryFindsPlantedBug is build-order step 2. A tool that cannot find a bug you
// planted cannot be trusted to find one you did not.
func TestDuplicateDeliveryFindsPlantedBug(t *testing.T) {
	t.Parallel()

	h := newHarness(t)

	clean := h.run(t, "dup-clean", replay.Clean{})

	cleanQty := h.qty(t)
	if want := toy.StartingQty - orderedQty; cleanQty != want {
		t.Fatalf("clean run left qty = %d, want %d", cleanQty, want)
	}

	mutated := h.run(t, "dup-mutated", replay.Duplicate{Seq: h.orderSeq})

	if want := toy.StartingQty - 2*orderedQty; h.qty(t) != want {
		t.Errorf("duplicate delivery left qty = %d, want %d; the planted bug did not fire", h.qty(t), want)
	}

	if mutated.Delivered <= clean.Delivered {
		t.Errorf("mutated run saw %d deliveries, clean saw %d; no redelivery happened",
			mutated.Delivered, clean.Delivered)
	}

	if result := gate.NewComparer().Compare(clean.Effects, mutated.Effects); result.OK() {
		t.Error("duplicate delivery produced an identical effect sequence; the bug is invisible")
	}
}

// TestIdempotentHandlerSurvivesDuplicate is the control. The same fault against a handler that
// assigns an absolute value must leave the same state, or the tool reports every handler as broken.
func TestIdempotentHandlerSurvivesDuplicate(t *testing.T) {
	t.Parallel()

	h := newHarness(t)

	const setQty = 7

	setSeq := h.publish(t, toy.SubjectStockSet, orderPayload("ORD-99002", h.sku, setQty))

	h.run(t, "control-clean", replay.Clean{})

	cleanQty := h.qty(t)
	if cleanQty != setQty {
		t.Fatalf("clean run left qty = %d, want %d", cleanQty, setQty)
	}

	h.run(t, "control-mutated", replay.Duplicate{Seq: setSeq})

	if mutatedQty := h.qty(t); mutatedQty != cleanQty {
		t.Errorf("idempotent handler left qty = %d under duplicate delivery, want %d unchanged",
			mutatedQty, cleanQty)
	}
}

// TestCrashBeforeAckAppliesTheSideEffectRepeatedly models a consumer that dies after doing the work
// and before reporting it, twice over, and confirms the loss compounds.
func TestCrashBeforeAckAppliesTheSideEffectRepeatedly(t *testing.T) {
	t.Parallel()

	h := newHarness(t)

	const crashes = 2

	result := h.run(t, "crash", replay.CrashBeforeAck{Seq: h.orderSeq, Times: crashes})

	if want := crashes + 1; result.Delivered != want {
		t.Errorf("saw %d deliveries, want %d (%d crashes then one success)", result.Delivered, want, crashes)
	}

	if want := toy.StartingQty - (crashes+1)*orderedQty; h.qty(t) != want {
		t.Errorf("qty = %d, want %d; the side effect did not compound across crashes", h.qty(t), want)
	}
}

// TestDelayCausesRedelivery holds a message past its governing deadline, which is what a slow
// consumer does to itself.
func TestDelayCausesRedelivery(t *testing.T) {
	t.Parallel()

	h := newHarness(t)

	result := h.run(t, "delayed", replay.Delay{Seq: h.orderSeq, For: testAckWait + time.Second})

	if result.Delivered < 2 {
		t.Errorf("saw %d deliveries; holding past the deadline should have caused a redelivery",
			result.Delivered)
	}
}

// TestReorderDispatchesTheLaterMessageFirst needs headroom for two outstanding messages, which is
// exactly the condition under which the bus stops guaranteeing order.
func TestReorderDispatchesTheLaterMessageFirst(t *testing.T) {
	t.Parallel()

	config := serialConfig()
	config.MaxAckPending = 2

	h := newHarnessWith(t, config)
	second := h.publish(t, toy.SubjectStockSet, orderPayload("ORD-99003", h.sku, 9))

	result := h.run(t, "reordered", replay.Reorder{First: h.orderSeq})

	if len(result.Effects) == 0 {
		t.Fatal("no effects observed")
	}

	if got := result.Effects[0].MessageSeq; got != second {
		t.Errorf("first effect came from message %d, want %d; the messages were not reordered", got, second)
	}
}

// TestGuardedHandlerSurvivesDuplicate is the false-positive control that matters most. The handler's
// side effect is the same non-idempotent decrement; only a claim on the message's stream sequence
// makes it safe. Stutter must not report it, and if it does the fault is Stutter's.
//
// This is the shape a real consumer uses, so getting it wrong here means getting it wrong in the
// field, where the report would be telling someone their working guard is broken.
func TestGuardedHandlerSurvivesDuplicate(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	guarded := h.publish(t, toy.SubjectOrderGuarded, orderPayload("ORD-99004", h.sku, orderedQty))

	h.run(t, "guarded-clean", replay.Clean{})

	cleanQty := h.qty(t)
	// The corpus also carries the unguarded order, so both handlers run on a clean pass.
	if want := toy.StartingQty - 2*orderedQty; cleanQty != want {
		t.Fatalf("clean run left qty = %d, want %d", cleanQty, want)
	}

	result := h.run(t, "guarded-mutated", replay.Duplicate{Seq: guarded})

	if result.Delivered <= 2 {
		t.Errorf("saw %d deliveries; the guarded message was not redelivered", result.Delivered)
	}

	if mutatedQty := h.qty(t); mutatedQty != cleanQty {
		t.Errorf("guarded handler left qty = %d under duplicate delivery, want %d unchanged; "+
			"the claim did not prevent the second application", mutatedQty, cleanQty)
	}
}

// TestRetainReplaysOnlyTheChosenMessages is what a shrinker needs: ask whether a failure still
// reproduces with fewer messages. Scoping is not a fault, so it bypasses the legality table.
func TestRetainReplaysOnlyTheChosenMessages(t *testing.T) {
	t.Parallel()

	directDSN := os.Getenv(envPostgres)
	if directDSN == "" {
		t.Skipf("%s is not set; skipping the test that needs a real database", envPostgres)
	}

	h := newHarness(t)
	kept := h.publish(t, toy.SubjectStockSet, orderPayload("ORD-99005", h.sku, 5))

	// Retain only the second message, so the first handler never runs and its decrement never
	// happens: the absolute set wins from the starting quantity rather than from a decremented one.
	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		t.Fatalf("generate hash key: %v", keyErr)
	}

	h.runner = replay.NewRunner(h.store, key, serialConfig(), replay.Options{
		Retain:  []uint64{kept},
		Quiesce: 50 * time.Millisecond,
	})

	result := h.run(t, "retained", replay.Clean{})

	if result.Delivered != 1 {
		t.Errorf("Delivered = %d, want 1; a message outside the retained set reached the handler",
			result.Delivered)
	}

	for _, observed := range result.Effects {
		if observed.MessageSeq != kept {
			t.Errorf("effect attributed to message %d, want only %d", observed.MessageSeq, kept)
		}
	}
}

// TestForbiddenFaultIsRefused is the core promise of the legality table: a fault the recorded
// configuration cannot produce is never injected, because a finding from an impossible fault is
// unfixable.
func TestForbiddenFaultIsRefused(t *testing.T) {
	t.Parallel()

	h := newHarness(t) // MaxAckPending 1, so ordering is guaranteed

	_, err := h.tryRun(t, "illegal-reorder", replay.Reorder{First: h.orderSeq})
	if !errors.Is(err, replay.ErrForbidden) {
		t.Errorf("Run() error = %v, want ErrForbidden", err)
	}
}

// TestConcurrentIsRefusedAsUnsupported keeps the gap visible. Injecting it today would mis-attribute
// effects and manufacture false positives, so it refuses rather than guessing.
func TestConcurrentIsRefusedAsUnsupported(t *testing.T) {
	t.Parallel()

	config := serialConfig()
	config.MaxAckPending = 4

	h := newHarnessWith(t, config)

	_, err := h.tryRun(t, "concurrent", replay.Concurrent{First: h.orderSeq, Second: h.orderSeq + 1})
	if !errors.Is(err, replay.ErrUnsupported) {
		t.Errorf("Run() error = %v, want ErrUnsupported", err)
	}
}
