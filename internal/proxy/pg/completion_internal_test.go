package pg

import (
	"slices"
	"testing"

	"github.com/Wintersta7e/stutter/internal/effect"
)

// verdicts is a sink that remembers what became of each token.
type verdicts struct {
	rejected []string
	answered []string
}

func (*verdicts) Record(effect.Observation) {}

func (v *verdicts) Reject(correlation string) { v.rejected = append(v.rejected, correlation) }

func (v *verdicts) Answered(correlation string) { v.answered = append(v.answered, correlation) }

// step is one message on the wire: a request the client sent, or an answer the server gave.
type step struct {
	sql    string
	token  string
	tag    string
	kind   request
	answer byte
	sent   bool
}

func sent(kind request, token, sql string) step {
	return step{kind: kind, token: token, sql: sql, sent: true}
}

func answered(msgType byte, tag string) step {
	return step{answer: msgType, tag: tag}
}

// ready is a ReadyForQuery reporting a transaction status. The runner appends a NUL to every answer;
// a status byte is read from the front, so the extra byte is harmless.
func ready(status byte) step {
	return answered(msgReadyForQuery, string(status))
}

// idle is a ReadyForQuery outside any transaction block.
func idle() step {
	return ready(txIdle)
}

// afterStartup prefixes the ReadyForQuery that ends every connection's startup, as a real connection
// has before its first request.
func afterStartup(steps ...step) []step {
	return append([]step{idle()}, steps...)
}

func TestAStatementThatChangedNothingIsRefused(t *testing.T) {
	t.Parallel()

	const (
		upsert = "-- name: InsertStat :exec\nINSERT INTO stats VALUES ($1) ON CONFLICT DO NOTHING"
		cte    = "WITH moved AS (SELECT 1 AS id) INSERT INTO stats (id) SELECT id FROM moved"
		purge  = "DELETE FROM stock"
		// Token names, and "begin" doubles as the statement it names.
		first = "first"
		claim = "claim"
		begin = "begin"
	)

	cases := []struct {
		name     string
		steps    []step
		rejected []string
		answered []string
	}{
		{
			name: "an insert that conflicted changed nothing; one that inserted did",
			steps: afterStartup(
				sent(requestExecute, first, upsert), sent(requestSync, "", ""),
				answered(msgCommandComplete, "INSERT 0 1"), idle(),
				sent(requestExecute, "again", upsert), sent(requestSync, "", ""),
				answered(msgCommandComplete, "INSERT 0 0"), idle(),
			),
			rejected: []string{"again"},
			answered: []string{first},
		},
		{
			name: "a ping is an empty query",
			steps: afterStartup(
				sent(requestSimple, "ping", "-- ping"),
				answered(msgEmptyQuery, ""), idle(),
			),
			rejected: []string{"ping"},
		},
		{
			name: "a read that found nothing still happened",
			steps: afterStartup(
				sent(requestExecute, "read", "SELECT qty FROM stock"), sent(requestSync, "", ""),
				answered(msgCommandComplete, "SELECT 0"), idle(),
			),
			answered: []string{"read"},
		},
		{
			// The pairing cannot be vouched for, so the statement stays counted.
			name: "a writing CTE is not matched to its tag",
			steps: afterStartup(
				sent(requestExecute, "cte", cte), sent(requestSync, "", ""),
				answered(msgCommandComplete, "INSERT 0 0"), idle(),
			),
			answered: []string{"cte"},
		},
		{
			name: "an error refuses its statement and everything skipped behind it",
			steps: afterStartup(
				sent(requestExecute, claim, "INSERT INTO processed VALUES ($1)"),
				sent(requestExecute, "work", "UPDATE stock SET qty = qty - 1"),
				sent(requestSync, "", ""),
				answered(msgErrorResponse, ""), idle(),
			),
			rejected: []string{claim, "work"},
		},
		{
			// Only the first statement can be matched to its tag; a second answer counts as a change.
			name: "a multi-statement query is not refused on a zero-row first answer",
			steps: afterStartup(
				sent(requestSimple, "multi", "DELETE FROM a; INSERT INTO b VALUES (1)"),
				answered(msgCommandComplete, "DELETE 0"), answered(msgCommandComplete, "INSERT 0 1"),
				idle(),
			),
			answered: []string{"multi"},
		},
		{
			name: "an answer nothing asked for stops the pairing, and nothing is refused after it",
			steps: afterStartup(
				answered(msgCommandComplete, "DELETE 0"),
				sent(requestExecute, "after", purge), sent(requestSync, "", ""),
				answered(msgCommandComplete, "DELETE 0"), idle(),
			),
			answered: []string{"after"},
		},
		{
			// Read as an answer, it would have broken the pairing before the first statement.
			name: "the ReadyForQuery that ends startup settles nothing",
			steps: afterStartup(
				sent(requestExecute, first, purge), sent(requestSync, "", ""),
				answered(msgCommandComplete, "DELETE 0"), idle(),
			),
			rejected: []string{first},
		},
		{
			// A unique-index dedupe guard under redelivery: the claim is refused and the handler rolls
			// back. BEGIN answered success, but the transaction it opened changed nothing.
			name: "a transaction that rolled back changed nothing, BEGIN and ROLLBACK included",
			steps: afterStartup(
				sent(requestSimple, begin, begin),
				answered(msgCommandComplete, "BEGIN"), ready(txOpen),
				sent(requestExecute, claim, "INSERT INTO processed VALUES ($1)"), sent(requestSync, "", ""),
				answered(msgErrorResponse, ""), ready(txFailed),
				sent(requestSimple, "rollback", "rollback"),
				answered(msgCommandComplete, "ROLLBACK"), idle(),
			),
			rejected: []string{begin, claim, "rollback"},
		},
		{
			name: "a committed transaction keeps each statement's own verdict",
			steps: afterStartup(
				sent(requestSimple, begin, begin),
				answered(msgCommandComplete, "BEGIN"), ready(txOpen),
				sent(requestExecute, claim, "INSERT INTO processed VALUES ($1) ON CONFLICT DO NOTHING"),
				sent(requestSync, "", ""),
				answered(msgCommandComplete, "INSERT 0 1"), ready(txOpen),
				sent(requestExecute, "stale", purge), sent(requestSync, "", ""),
				answered(msgCommandComplete, "DELETE 0"), ready(txOpen),
				sent(requestSimple, "commit", "commit"),
				answered(msgCommandComplete, "COMMIT"), idle(),
			),
			rejected: []string{"stale"},
			answered: []string{begin, claim, "commit"},
		},
		{
			name: "a COMMIT of a failed transaction is a rollback",
			steps: afterStartup(
				sent(requestSimple, begin, begin),
				answered(msgCommandComplete, "BEGIN"), ready(txOpen),
				sent(requestExecute, claim, "INSERT INTO processed VALUES ($1)"), sent(requestSync, "", ""),
				answered(msgErrorResponse, ""), ready(txFailed),
				sent(requestSimple, "commit", "commit"),
				answered(msgCommandComplete, "ROLLBACK"), idle(),
			),
			rejected: []string{begin, claim, "commit"},
		},
		{
			// With no explicit transaction a batch is one implicit transaction, and an error in it takes
			// the statements that already succeeded down with it.
			name: "an error undoes the implicit transaction of its batch",
			steps: afterStartup(
				sent(requestExecute, first, "INSERT INTO ledger VALUES ($1)"),
				sent(requestExecute, "second", "INSERT INTO processed VALUES ($1)"),
				sent(requestSync, "", ""),
				answered(msgCommandComplete, "INSERT 0 1"), answered(msgErrorResponse, ""), idle(),
			),
			rejected: []string{first, "second"},
		},
		{
			name: "a request queued before startup ended stops the pairing",
			steps: []step{
				sent(requestExecute, "early", purge), sent(requestSync, "", ""),
				idle(),
				answered(msgCommandComplete, "DELETE 0"), idle(),
			},
			answered: []string{"early"},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			sink := &verdicts{}
			pairing := &completions{sink: sink}

			for _, next := range testCase.steps {
				if next.sent {
					pairing.expect(next.kind, next.token, next.sql)

					continue
				}

				pairing.settle(next.answer, append([]byte(next.tag), 0))
			}

			if !slices.Equal(sink.rejected, testCase.rejected) {
				t.Errorf("rejected = %q, want %q", sink.rejected, testCase.rejected)
			}

			if !slices.Equal(sink.answered, testCase.answered) {
				t.Errorf("answered = %q, want %q", sink.answered, testCase.answered)
			}
		})
	}
}

func TestLeadingVerbSkipsComments(t *testing.T) {
	t.Parallel()

	for sql, want := range map[string]string{
		"-- name: InsertStat :exec\nINSERT INTO stats": "INSERT",
		"/* tagged */ update stock SET qty = 1":        "UPDATE",
		"  \n\tDELETE FROM stock":                      "DELETE",
		"-- ping":                                      "",
	} {
		if got := leadingVerb(sql); got != want {
			t.Errorf("leadingVerb(%q) = %q, want %q", sql, got, want)
		}
	}
}
