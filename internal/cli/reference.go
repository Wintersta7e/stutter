package cli

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/report"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// errNoDatabase means the command was run without somewhere to replay against.
var errNoDatabase = errors.New("--postgres is required: stutter replays into a real database")

// referenceSKU is the stock item the reference consumer moves. Fixture state is scoped to it, so a
// run does not disturb anything else in the database it is pointed at.
const referenceSKU = "STUTTER-REFERENCE-WIDGET"

const (
	referenceQty = 3
	absoluteQty  = 7
)

// execute provisions the reference consumer and checks it.
func execute(ctx context.Context, parsed settings, gatesOnly bool) (report.Report, error) {
	store, cleanup, err := startCorpus(ctx)
	if err != nil {
		return report.Report{}, err
	}

	defer cleanup()

	messages, err := publishReference(ctx, store)
	if err != nil {
		return report.Report{}, err
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		return report.Report{}, fmt.Errorf("generate the effect hash key: %w", keyErr)
	}

	config := referenceConfig()

	sandbox, err := harness.New(toy.SandboxConfig(store, parsed.postgres, referenceSKU, config, key))
	if err != nil {
		return report.Report{}, fmt.Errorf("provision the sandbox: %w", err)
	}

	result, err := check.Run(ctx, sandbox, check.Options{
		Messages:  messages,
		Consumer:  parsed.consumer,
		Config:    config,
		MaxRuns:   parsed.maxRuns,
		GatesOnly: gatesOnly,
	})
	if err != nil {
		return report.Report{}, fmt.Errorf("run the check: %w", err)
	}

	return result, nil
}

func startCorpus(ctx context.Context) (*corpus.Corpus, func(), error) {
	dir, err := os.MkdirTemp("", "stutter-corpus-")
	if err != nil {
		return nil, nil, fmt.Errorf("create a corpus directory: %w", err)
	}

	store, err := corpus.Start(ctx, dir)
	if err != nil {
		_ = os.RemoveAll(dir)

		return nil, nil, fmt.Errorf("start the corpus: %w", err)
	}

	cleanup := func() {
		store.Close()

		_ = os.RemoveAll(dir)
	}

	return store, cleanup, nil
}

// publishReference writes the reference corpus: one message per handler, so a report shows a
// failure and its controls side by side rather than a failure alone.
func publishReference(ctx context.Context, store *corpus.Corpus) ([]uint64, error) {
	written := []struct {
		subject string
		payload []byte
	}{
		{toy.SubjectOrderCreated, order("REF-1", referenceQty)},
		{toy.SubjectStockSet, order("REF-2", absoluteQty)},
		{toy.SubjectOrderGuarded, order("REF-3", referenceQty)},
	}

	messages := make([]uint64, 0, len(written))

	for _, message := range written {
		seq, err := store.Publish(ctx, message.subject, message.payload)
		if err != nil {
			return nil, fmt.Errorf("publish the reference corpus: %w", err)
		}

		messages = append(messages, seq)
	}

	return messages, nil
}

func order(id string, qty int) []byte {
	return fmt.Appendf(nil, `{"order_id":%q,"sku":%q,"qty":%d}`, id, referenceSKU, qty)
}
