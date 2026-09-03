package toy

import (
	"context"
	"time"

	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/policy"
)

// DefaultQuiesce is how long the reference consumer's attribution window stays open after a handler
// returns. Its handlers are synchronous, so this only has to cover scheduling.
const DefaultQuiesce = 50 * time.Millisecond

// guarded closes the consumer and its claim guard together.
//
// Ordering matters: both proxies wait for in-flight connections when they close, so a guard holding
// an idle bus connection would turn teardown into a hang rather than an error.
type guarded struct {
	*Consumer

	guard *Guard
}

func (g *guarded) Close(ctx context.Context) {
	g.guard.Close()
	g.Consumer.Close(ctx)
}

// SandboxConfig wires the reference consumer into a harness.
//
// Reset uses DIRECT addresses because fixture writes are not the service's behaviour and recording
// them would put noise into every effect sequence. Connect uses the PROXIED addresses, the bus
// included: the guard's claim is a publish, and unobserved it would be invisible.
func SandboxConfig(store *corpus.Corpus, directDSN, sku string, config policy.Config, hashKey []byte) harness.Config {
	return harness.Config{
		Corpus:      store,
		PostgresDSN: directDSN,
		HashKey:     hashKey,
		Policy:      config,
		Quiesce:     DefaultQuiesce,

		Reset: func(ctx context.Context) error {
			if err := Setup(ctx, directDSN, sku); err != nil {
				return err
			}

			guard, err := NewGuard(ctx, store.URL())
			if err != nil {
				return err
			}

			defer guard.Close()

			return guard.Reset(ctx)
		},

		Connect: func(ctx context.Context, postgresDSN, natsURL string) (harness.Service, error) {
			consumer, err := Connect(ctx, postgresDSN)
			if err != nil {
				return nil, err
			}

			guard, err := NewGuard(ctx, natsURL)
			if err != nil {
				consumer.Close(ctx)

				return nil, err
			}

			consumer.UseGuard(guard)

			return &guarded{Consumer: consumer, guard: guard}, nil
		},
	}
}
