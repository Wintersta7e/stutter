package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/Wintersta7e/stutter/internal/check"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/report"
	"github.com/Wintersta7e/stutter/internal/toy"
)

// referenceSKU prefixes the stock item the reference consumer moves. Fixture state is scoped to one
// item per invocation, so a run disturbs nothing else in the database it is pointed at — another
// invocation's fixture included.
const referenceSKU = "STUTTER-REFERENCE-WIDGET"

// referenceConsumer is the consumer the reference path's findings attribute to: the one consumer the
// reference corpus is checked against, named, never a flag.
const referenceConsumer = "reserve_stock"

// skuSuffixLen is how many random bytes make one invocation's stock item its own.
const skuSuffixLen = 8

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

	sku, err := newReferenceSKU()
	if err != nil {
		return report.Report{}, err
	}

	messages, err := publishReference(ctx, store, sku)
	if err != nil {
		return report.Report{}, err
	}

	key := make([]byte, hashKeyLen)
	if _, keyErr := rand.Read(key); keyErr != nil {
		return report.Report{}, fmt.Errorf("generate the effect hash key: %w", keyErr)
	}

	config := referenceConfig()

	sandboxConfig := toy.SandboxConfig(store, parsed.postgres.value, sku, config, key)
	if parsed.quiesce.set {
		sandboxConfig.Quiesce = parsed.quiesce.value
	}

	sandbox, err := harness.New(sandboxConfig)
	if err != nil {
		return report.Report{}, fmt.Errorf("provision the sandbox: %w", err)
	}

	result, err := check.Run(ctx, sandbox, check.Options{
		Messages:  messages,
		Consumer:  referenceConsumer,
		Config:    config,
		MaxRuns:   parsed.maxRuns.value,
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
func publishReference(ctx context.Context, store *corpus.Corpus, sku string) ([]uint64, error) {
	written := []struct {
		subject string
		payload []byte
	}{
		{toy.SubjectOrderCreated, order("REF-1", sku, referenceQty)},
		{toy.SubjectStockSet, order("REF-2", sku, absoluteQty)},
		{toy.SubjectOrderGuarded, order("REF-3", sku, referenceQty)},
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

func order(id, sku string, qty int) []byte {
	return fmt.Appendf(nil, `{"order_id":%q,"sku":%q,"qty":%d}`, id, sku, qty)
}

// newReferenceSKU is one invocation's own stock item.
func newReferenceSKU() (string, error) {
	suffix := make([]byte, skuSuffixLen)
	if _, err := rand.Read(suffix); err != nil {
		return "", fmt.Errorf("generate the fixture's stock item: %w", err)
	}

	return referenceSKU + "-" + hex.EncodeToString(suffix), nil
}
