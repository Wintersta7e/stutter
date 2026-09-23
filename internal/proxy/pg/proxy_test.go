package pg_test

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

const envPostgres = "STUTTER_TEST_POSTGRES"

// through connects a real client to a real database via the proxy, with a window already open so
// every statement is recorded against one message.
func through(t *testing.T) (*pgx.Conn, *effect.Recorder) {
	t.Helper()

	dsn := os.Getenv(envPostgres)
	if dsn == "" {
		t.Skipf("%s is not set; skipping the test that needs a real database", envPostgres)
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse %s: %v", envPostgres, err)
	}

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), make([]byte, 32))

	proxy, err := pg.Listen(t.Context(), "127.0.0.1:0", parsed.Host, recorder)
	if err != nil {
		t.Fatalf("pg.Listen() error = %v", err)
	}

	served := make(chan error, 1)

	go func() { served <- proxy.Serve(t.Context()) }()

	parsed.Host = proxy.Addr()

	query := parsed.Query()
	query.Set("sslmode", "disable")
	parsed.RawQuery = query.Encode()

	conn, err := pgx.Connect(t.Context(), parsed.String())
	if err != nil {
		t.Fatalf("connect through the proxy: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close(context.Background())

		if closeErr := proxy.Close(); closeErr != nil {
			t.Errorf("proxy.Close() error = %v", closeErr)
		}

		if serveErr := <-served; serveErr != nil {
			t.Errorf("proxy.Serve() error = %v", serveErr)
		}
	})

	return conn, recorder
}

// TestTheDatabaseSaysWhichStatementsChangedNothing drives the real protocol, as a real client speaks
// it, and checks what each statement is ruled to have done. The shape is the real target's control
// consumer: an insert whose ON CONFLICT DO NOTHING makes a redelivery harmless.
func TestTheDatabaseSaysWhichStatementsChangedNothing(t *testing.T) {
	t.Parallel()

	conn, recorder := through(t)

	table := "completion_" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "_"))

	// Setup runs before the window opens, so none of it is an effect.
	for _, setup := range []string{
		"DROP TABLE IF EXISTS " + table,
		"CREATE TABLE " + table + " (id int PRIMARY KEY, n int NOT NULL DEFAULT 0)",
	} {
		if _, err := conn.Exec(t.Context(), setup); err != nil {
			t.Fatalf("%s: %v", setup, err)
		}
	}

	t.Cleanup(func() {
		if _, err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+table); err != nil {
			t.Errorf("drop %s: %v", table, err)
		}
	})

	recorder.Open("probe", 1, nil)

	upsert := "INSERT INTO " + table + " (id) VALUES ($1) ON CONFLICT (id) DO NOTHING"

	statements := []struct {
		sql      string
		args     []any
		rejected bool
	}{
		{sql: upsert, args: []any{1}, rejected: false},
		{sql: upsert, args: []any{1}, rejected: true},
		{sql: "UPDATE " + table + " SET n = n + 1 WHERE id = $1", args: []any{1}, rejected: false},
		{sql: "UPDATE " + table + " SET n = n + 1 WHERE id = $1", args: []any{2}, rejected: true},
		{sql: "SELECT id FROM " + table + " WHERE id = $1", args: []any{2}, rejected: false},
		{sql: "INSERT INTO " + table + " (id) VALUES ($1)", args: []any{1}, rejected: true},
	}

	for _, statement := range statements {
		rows, err := conn.Query(t.Context(), statement.sql, statement.args...)
		if err == nil {
			rows.Close()
			err = rows.Err()
		}

		// The last insert violates the primary key on purpose: that refusal is how a unique-index
		// dedupe guard says a message was already handled.
		if err != nil && !statement.rejected {
			t.Fatalf("%s %v: %v", statement.sql, statement.args, err)
		}
	}

	if err := conn.Ping(t.Context()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	observed := recorder.Effects()
	if len(observed) != len(statements)+1 {
		t.Fatalf("recorded %d effects, want %d:\n%v", len(observed), len(statements)+1, observed)
	}

	for at, statement := range statements {
		if observed[at].Rejected != statement.rejected {
			t.Errorf("effect %d %q: Rejected = %v, want %v", at, observed[at].Canonical,
				observed[at].Rejected, statement.rejected)
		}
	}

	if ping := observed[len(statements)]; !ping.Rejected {
		t.Errorf(
			"the ping %q was counted as a change: a pooled connection's health check is not the handler",
			ping.Canonical,
		)
	}
}

// TestARolledBackTransactionChangedNothing is a unique-index dedupe guard as a real client runs it: on
// redelivery the claim is refused and the transaction rolled back, so BEGIN, the claim and the
// ROLLBACK changed nothing, while a transaction that committed keeps its work counted.
func TestARolledBackTransactionChangedNothing(t *testing.T) {
	t.Parallel()

	conn, recorder := through(t)

	table := "rollback_" + strings.ToLower(t.Name())

	for _, setup := range []string{
		"DROP TABLE IF EXISTS " + table,
		"CREATE TABLE " + table + " (id int PRIMARY KEY)",
	} {
		if _, err := conn.Exec(t.Context(), setup); err != nil {
			t.Fatalf("%s: %v", setup, err)
		}
	}

	t.Cleanup(func() {
		if _, err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+table); err != nil {
			t.Errorf("drop %s: %v", table, err)
		}
	})

	recorder.Open("probe", 1, nil)

	claim := "INSERT INTO " + table + " (id) VALUES ($1)"

	// First delivery: the claim succeeds and the transaction commits.
	committed, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}

	if _, claimErr := committed.Exec(t.Context(), claim, 1); claimErr != nil {
		t.Fatalf("first claim: %v", claimErr)
	}

	if commitErr := committed.Commit(t.Context()); commitErr != nil {
		t.Fatalf("Commit() error = %v", commitErr)
	}

	wrote := len(recorder.Effects())

	// Redelivery: the claim violates the key and the handler rolls back.
	undone, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}

	if _, err := undone.Exec(t.Context(), claim, 1); err == nil {
		t.Fatal("the second claim succeeded; the guard under test never refused anything")
	}

	if err := undone.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	observed := recorder.Effects()
	if wrote == 0 || len(observed) == wrote {
		t.Fatalf("recorded %d effects for the commit and %d in all; the proxy saw too little", wrote, len(observed))
	}

	for at, item := range observed {
		if want := at >= wrote; item.Rejected != want {
			t.Errorf("effect %d %q: Rejected = %v, want %v", at, item.Canonical, item.Rejected, want)
		}
	}
}
