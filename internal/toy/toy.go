// Package toy is a deliberately flawed message consumer used to exercise Stutter against a bug
// whose location is already known.
//
// It carries one non-idempotent handler, one idempotent control, and one guarded variant of the
// non-idempotent handler. A tool that cannot find a bug you planted cannot be trusted to find one
// you did not, and a tool that flags the guarded handler is producing the false positive that
// matters most — so this package is the yardstick in both directions rather than a demonstration of
// good practice.
//
// It is also deliberately nondeterministic in the ordinary ways — a client-generated identifier and
// a wall-clock stamp on every audit row — because a trivially deterministic consumer would pass the
// determinism gate while proving nothing about the normaliser.
package toy

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Wintersta7e/stutter/internal/replay"
)

// Subjects the toy consumer handles.
const (
	// SubjectOrderCreated routes to the non-idempotent handler.
	SubjectOrderCreated = "corpus.order.created"
	// SubjectStockSet routes to the idempotent control.
	SubjectStockSet = "corpus.stock.set"
	// SubjectOrderGuarded routes to the non-idempotent handler behind a claim guard, which makes it
	// safe under redelivery without making the side effect itself idempotent.
	SubjectOrderGuarded = "corpus.order.guarded"
)

// StartingQty is the stock level every run begins from.
const StartingQty = 14

// errNoGuard means a guarded message arrived at a consumer that has no guard wired in.
var errNoGuard = errors.New("guarded subject received but no guard is configured")

// Order is the message payload both handlers accept.
type Order struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
	Qty     int    `json:"qty"`
}

// Setup creates the schema and resets one SKU to the starting position.
//
// It must run on a direct connection, never through the proxy: setup writes are not the service's
// behaviour and recording them would put fixture noise into every effect sequence.
//
// Reset is scoped to a single SKU rather than truncating, so that concurrent runs against one
// database do not clobber each other's fixture. A shared reset is a race that shows up as a
// divergence the service never caused.
func Setup(ctx context.Context, directDSN, sku string) error {
	conn, err := pgx.Connect(ctx, directDSN)
	if err != nil {
		return fmt.Errorf("connect for setup: %w", err)
	}

	defer func() { _ = conn.Close(ctx) }()

	if err := createSchema(ctx, conn); err != nil {
		return err
	}

	if _, err := conn.Exec(ctx,
		`INSERT INTO stock (sku, qty) VALUES ($1, $2)
		 ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty`, sku, StartingQty); err != nil {
		return fmt.Errorf("reset stock for %q: %w", sku, err)
	}

	return nil
}

// schemaLock serialises schema creation across concurrent setups.
//
// CREATE TABLE IF NOT EXISTS is NOT atomic in Postgres: it checks the catalogue and then inserts, so
// two sessions running it at the same moment race and the loser fails with a duplicate key on
// pg_type_typname_nsp_index. Serial local runs never hit it; parallel tests against one database do.
const schemaLock int64 = 0x57_49_44_47 // "WIDG"

func createSchema(ctx context.Context, conn *pgx.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin schema transaction: %w", err)
	}

	defer func() {
		// After a successful commit this returns ErrTxClosed, which is the normal path, and a
		// deferred call has nowhere to report anything to in any case.
		_ = tx.Rollback(ctx) //nolint:errcheck // expected ErrTxClosed once committed.
	}()

	// Released automatically when the transaction ends, however it ends.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, schemaLock); err != nil {
		return fmt.Errorf("take the schema lock: %w", err)
	}

	schema := []string{
		`CREATE TABLE IF NOT EXISTS stock (sku TEXT PRIMARY KEY, qty INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS audit (id TEXT PRIMARY KEY, order_id TEXT NOT NULL, at TIMESTAMPTZ NOT NULL)`,
	}

	for _, statement := range schema {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("setup statement %q: %w", statement, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit schema transaction: %w", err)
	}

	return nil
}

// Qty reads a SKU's current stock level on a direct connection.
func Qty(ctx context.Context, directDSN, sku string) (int, error) {
	conn, err := pgx.Connect(ctx, directDSN)
	if err != nil {
		return 0, fmt.Errorf("connect to read stock: %w", err)
	}

	defer func() { _ = conn.Close(ctx) }()

	var qty int
	if err := conn.QueryRow(ctx, `SELECT qty FROM stock WHERE sku = $1`, sku).Scan(&qty); err != nil {
		return 0, fmt.Errorf("read stock for %q: %w", sku, err)
	}

	return qty, nil
}

// Consumer handles messages against a database reached through the proxy.
type Consumer struct {
	conn  *pgx.Conn
	guard *Guard
}

// Connect opens the consumer's database connection.
//
// dsn must point at the proxy, not at the database, or no effect is observed.
func Connect(ctx context.Context, dsn string) (*Consumer, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect through proxy: %w", err)
	}

	return &Consumer{conn: conn}, nil
}

// UseGuard wires in the claim guard that SubjectOrderGuarded needs.
func (c *Consumer) UseGuard(guard *Guard) {
	c.guard = guard
}

// Close releases the connection.
func (c *Consumer) Close(ctx context.Context) {
	_ = c.conn.Close(ctx)
}

// Handle routes a message to its handler.
func (c *Consumer) Handle(ctx context.Context, msg replay.Message) error {
	var order Order
	if err := json.Unmarshal(msg.Payload, &order); err != nil {
		return fmt.Errorf("decode message %d: %w", msg.Seq, err)
	}

	switch msg.Subject {
	case SubjectOrderCreated:
		return c.reserveStock(ctx, order)
	case SubjectStockSet:
		return c.setStock(ctx, order)
	case SubjectOrderGuarded:
		return c.reserveStockGuarded(ctx, msg, order)
	default:
		return nil
	}
}

// reserveStock is NOT idempotent. It decrements by the ordered quantity every time it runs, so a
// second delivery of the same order removes the stock twice. This is the planted bug.
func (c *Consumer) reserveStock(ctx context.Context, order Order) error {
	if err := c.audit(ctx, order); err != nil {
		return err
	}

	if _, err := c.conn.Exec(ctx,
		`UPDATE stock SET qty = qty - $1 WHERE sku = $2`, order.Qty, order.SKU); err != nil {
		return fmt.Errorf("reserve stock: %w", err)
	}

	return nil
}

// reserveStockGuarded applies the same non-idempotent decrement behind a claim on the message's
// stream sequence, so a redelivery finds the claim taken and does nothing.
//
// The side effect is unchanged; only the guard makes it safe. Stutter must not report this handler,
// and if it does, the fault is Stutter's.
func (c *Consumer) reserveStockGuarded(ctx context.Context, msg replay.Message, order Order) error {
	if c.guard == nil {
		return errNoGuard
	}

	claimed, err := c.guard.Claim(ctx, msg.Seq)
	if err != nil {
		return err
	}

	if !claimed {
		return nil
	}

	return c.reserveStock(ctx, order)
}

// setStock IS idempotent. It assigns an absolute level, so running it twice leaves the same result
// as running it once. This is the control.
func (c *Consumer) setStock(ctx context.Context, order Order) error {
	if err := c.audit(ctx, order); err != nil {
		return err
	}

	if _, err := c.conn.Exec(ctx,
		`UPDATE stock SET qty = $1 WHERE sku = $2`, order.Qty, order.SKU); err != nil {
		return fmt.Errorf("set stock: %w", err)
	}

	return nil
}

// audit writes a row carrying a client-generated identifier and a wall-clock stamp.
//
// Both values change on every run by design: without them the determinism gate would pass on a
// consumer that never produced anything for the normaliser to normalise.
func (c *Consumer) audit(ctx context.Context, order Order) error {
	id, err := newID()
	if err != nil {
		return err
	}

	if _, err := c.conn.Exec(ctx,
		`INSERT INTO audit (id, order_id, at) VALUES ($1, $2, $3)`,
		id, order.OrderID, time.Now().UTC()); err != nil {
		return fmt.Errorf("write audit row: %w", err)
	}

	return nil
}

// newID returns a random RFC 4122 version 4 identifier.
func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate identifier: %w", err)
	}

	const (
		versionByte = 6
		variantByte = 8
		// RFC 4122: the high nibble of byte 6 is the version, the top bits of byte 8 the variant.
		versionMask = 0x0f
		versionFour = 0x40
		variantMask = 0x3f
		variantRFC  = 0x80
	)

	raw[versionByte] = (raw[versionByte] & versionMask) | versionFour
	raw[variantByte] = (raw[variantByte] & variantMask) | variantRFC

	return fmt.Sprintf("%x-%x-%x-%x-%x",
		raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}
