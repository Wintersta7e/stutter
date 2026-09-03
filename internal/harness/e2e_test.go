package harness_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
	"github.com/Wintersta7e/stutter/internal/replay"
	"github.com/Wintersta7e/stutter/internal/report"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// counting wraps a session so a test can prove a fault was actually injected.
//
// It exists because a control that passes with nothing having run is indistinguishable from a
// control that passes because the handler is safe, and only one of those is worth anything.
type counting struct {
	inner  check.Session
	faults int
}

func (c *counting) Reset(ctx context.Context) error {
	return c.inner.Reset(ctx)
}

func (c *counting) Run(
	ctx context.Context,
	name string,
	mutation replay.Mutation,
	retain []uint64,
) (replay.Result, error) {
	if mutation.Fault() != policy.FaultNone {
		c.faults++
	}

	return c.inner.Run(ctx, name, mutation, retain)
}

const (
	envPostgres = "STUTTER_TEST_POSTGRES"
	hashKeyLen  = 32
	orderedQty  = 3
)

// guarded closes both the consumer and its claim guard. The guard holds its own bus connection, and
// both must be released before the proxies are: a proxy's Close waits for in-flight connections, so
// an idle open one turns teardown into a hang rather than an error.
type guarded struct {
	*toy.Consumer

	guard *toy.Guard
}

func (g *guarded) Close(ctx context.Context) {
	g.guard.Close()
	g.Consumer.Close(ctx)
}

func sandbox(t *testing.T, sku string, store *corpus.Corpus, config policy.Config) *harness.Sandbox {
	t.Helper()

	directDSN := os.Getenv(envPostgres)

	key := make([]byte, hashKeyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate hash key: %v", err)
	}

	built, err := harness.New(harness.Config{
		Corpus:      store,
		PostgresDSN: directDSN,
		HashKey:     key,
		Policy:      config,
		Quiesce:     50 * time.Millisecond,

		// Reset uses the DIRECT addresses: fixture writes are not the service's behaviour, and
		// recording them would put noise into every effect sequence.
		Reset: func(ctx context.Context) error {
			if err := toy.Setup(ctx, directDSN, sku); err != nil {
				return err
			}

			guard, err := toy.NewGuard(ctx, store.URL())
			if err != nil {
				return err
			}

			defer guard.Close()

			return guard.Reset(ctx)
		},

		// Connect uses the PROXIED addresses. Routing the guard through the bus proxy is the whole
		// point: its claim is a publish to $KV, and unobserved it would be invisible.
		Connect: func(ctx context.Context, postgresDSN, natsURL string) (harness.Service, error) {
			consumer, err := toy.Connect(ctx, postgresDSN)
			if err != nil {
				return nil, err
			}

			guard, err := toy.NewGuard(ctx, natsURL)
			if err != nil {
				consumer.Close(ctx)

				return nil, err
			}

			consumer.UseGuard(guard)

			return &guarded{Consumer: consumer, guard: guard}, nil
		},
	})
	if err != nil {
		t.Fatalf("harness.New() error = %v", err)
	}

	return built
}

func orderPayload(orderID, sku string, qty int) []byte {
	return fmt.Appendf(nil, `{"order_id":%q,"sku":%q,"qty":%d}`, orderID, sku, qty)
}

// TestCheckReportsThePlantedBugEndToEnd is the whole tool in one test: record, replay through both
// proxies, gate, inject every permitted fault, shrink, and rule. Nothing here is stubbed — a real
// Postgres, a real embedded bus, and a consumer that genuinely loses stock.
func TestCheckReportsThePlantedBugEndToEnd(t *testing.T) {
	t.Parallel()

	if os.Getenv(envPostgres) == "" {
		t.Skipf("%s is not set; skipping the test that needs a real database", envPostgres)
	}

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	sku := "WIDGET-" + t.Name()

	seq, err := store.Publish(t.Context(), toy.SubjectOrderCreated, orderPayload("ORD-1", sku, orderedQty))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	// One message and a serial consumer: reorder and concurrency are forbidden by the config, so
	// the run stays short while still exercising every fault that is legal here.
	config := policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{"corpus.>"},
		AckWait:        time.Second,
		MaxDeliver:     6,
		MaxAckPending:  1,
	}

	result, err := check.Run(t.Context(), sandbox(t, sku, store, config), check.Options{
		Messages: []uint64{seq},
		Consumer: "reserve_stock",
		Config:   config,
		MaxRuns:  4,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	rendered := result.String()
	t.Logf("report:\n%s", rendered)

	if got := result.ExitCode(); got != 1 {
		t.Fatalf("ExitCode() = %d, want 1 — the planted bug was not reported\n%s", got, rendered)
	}

	if len(result.Findings) == 0 {
		t.Fatal("no findings")
	}

	found := result.Findings[0]
	if found.Status != report.StatusFail {
		t.Errorf("Status = %q, want FAIL (reservations: %v)", found.Status, found.Reservations)
	}

	if found.Clause == "" {
		t.Error("the finding carries no configuration clause, so a user cannot trace why the fault was legal")
	}

	if found.Repro == "" {
		t.Error("the finding carries no minimal repro")
	}

	if !strings.Contains(rendered, "FAIL") {
		t.Errorf("rendered report has no FAIL line:\n%s", rendered)
	}
}

// TestGuardedHandlerIsNotReported is the false-positive control, now running through the full check
// rather than by inspecting stock directly. A handler made safe by a claim must produce a clean
// report; if it does not, the tool is telling someone their working guard is broken.
func TestGuardedHandlerIsNotReported(t *testing.T) {
	t.Parallel()

	if os.Getenv(envPostgres) == "" {
		t.Skipf("%s is not set; skipping the test that needs a real database", envPostgres)
	}

	store, err := corpus.Start(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("corpus.Start() error = %v", err)
	}

	t.Cleanup(store.Close)

	sku := "WIDGET-" + t.Name()

	seq, err := store.Publish(t.Context(), toy.SubjectOrderGuarded, orderPayload("ORD-2", sku, orderedQty))
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	config := policy.Config{
		AckMode:        policy.AckExplicit,
		FilterSubjects: []string{"corpus.>"},
		AckWait:        time.Second,
		MaxDeliver:     6,
		MaxAckPending:  1,
	}

	session := &counting{inner: sandbox(t, sku, store, config)}

	result, err := check.Run(t.Context(), session, check.Options{
		Messages: []uint64{seq},
		Consumer: "reserve_stock_guarded",
		Config:   config,
		MaxRuns:  2,
	})
	if err != nil {
		t.Fatalf("check.Run() error = %v", err)
	}

	t.Logf("report:\n%s", result)

	if session.faults == 0 {
		t.Fatal("no fault was injected, so a clean report proves nothing about the guard")
	}

	for _, found := range result.Findings {
		if found.Status == report.StatusFail {
			t.Errorf("a correctly guarded handler was reported as failing:\n%s", result)
		}
	}
}
