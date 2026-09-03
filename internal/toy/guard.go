package toy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// claimBucket holds one entry per message whose side effect has been applied.
	claimBucket = "stutter-toy-claims"
	// claimTTL has to outlast the window in which the bus can still redeliver a message, or the
	// guard forgets a claim exactly when it is needed and the side effect applies twice.
	claimTTL = time.Hour
)

// errBucketGone means the guard's bucket was already absent when Reset ran.
var errBucketGone = errors.New("guard bucket not found")

// Guard admits at most one application of a message's side effect.
//
// It mirrors the shape a real consumer uses: an atomic create in the bus's own key/value store,
// keyed on the stream sequence, taken before the work starts. The stream sequence is the right key
// because every redelivery of one stored message carries the same one, so they collapse onto a
// single claim.
//
// This exists as a control, not a demonstration. A correctly guarded handler must NOT be reported
// as non-idempotent; if Stutter flags it, Stutter is wrong.
type Guard struct {
	conn   *nats.Conn
	stream jetstream.JetStream
	kv     jetstream.KeyValue
}

// NewGuard opens the guard's bucket on the bus.
func NewGuard(ctx context.Context, natsURL string) (*Guard, error) {
	conn, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("connect to the bus for the guard: %w", err)
	}

	stream, err := jetstream.New(conn)
	if err != nil {
		conn.Close()

		return nil, fmt.Errorf("open jetstream context for the guard: %w", err)
	}

	bucket, err := openBucket(ctx, stream)
	if err != nil {
		conn.Close()

		return nil, err
	}

	return &Guard{conn: conn, stream: stream, kv: bucket}, nil
}

// openBucket creates the guard's bucket, or opens the existing one.
//
//nolint:ireturn // the client library models a bucket as an interface.
func openBucket(ctx context.Context, stream jetstream.JetStream) (jetstream.KeyValue, error) {
	bucket, err := stream.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      claimBucket,
		Description: "Messages whose side effect has been applied, so a redelivery does not apply it again",
		TTL:         claimTTL,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("open guard bucket %q: %w", claimBucket, err)
	}

	return bucket, nil
}

// Close releases the guard's connection.
func (g *Guard) Close() {
	g.conn.Close()
}

// Reset forgets every claim by deleting the bucket outright.
//
// Run isolation has to cover state the handler keeps on the bus, not just its datastore. A claim
// left over from the reference run makes the faulted run skip work it should have done, and the
// comparison then reports the opposite of the truth: a broken guard looks safe, and a working one
// looks like it never ran.
//
// Deleting rather than purging, because purging is not enough. A purged key keeps a marker, and the
// client's Create against a purged key falls back to a revision-checked update — three publishes
// where a virgin key takes one. The bucket was semantically empty and still produced a different
// wire sequence, which the determinism gate correctly refused. Run isolation must restore a
// byte-identical starting state, not merely an equivalent one.
// The bucket is re-opened afterwards so the guard stays usable: a caller that reset before handling
// would otherwise hold a handle to a bucket that no longer exists.
func (g *Guard) Reset(ctx context.Context) error {
	if err := g.stream.DeleteKeyValue(ctx, claimBucket); err != nil {
		if !errors.Is(err, jetstream.ErrBucketNotFound) {
			return fmt.Errorf("%w: delete %q: %w", errBucketGone, claimBucket, err)
		}
	}

	bucket, err := openBucket(ctx, g.stream)
	if err != nil {
		return err
	}

	g.kv = bucket

	return nil
}

// Claim reserves a message's side effect for this delivery.
//
// It reports false when the claim was already taken, which means another delivery of the same
// stored message has already done — or is doing — the work.
func (g *Guard) Claim(ctx context.Context, seq uint64) (bool, error) {
	key := strconv.FormatUint(seq, 10)

	if _, err := g.kv.Create(ctx, key, []byte("claimed")); err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return false, nil
		}

		return false, fmt.Errorf("claim message %d: %w", seq, err)
	}

	return true, nil
}
