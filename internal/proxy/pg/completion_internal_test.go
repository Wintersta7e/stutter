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

// afterStartup prefixes the ReadyForQuery that ends every connection's startup, as a real connection
// has before its first request.
func afterStartup(steps ...step) []step {
	return append([]step{answered(msgReadyForQuery, "")}, steps...)
}

func TestAStatementThatChangedNothingIsRefused(t *testing.T) {
	t.Parallel()

	const (
		upsert = "-- name: InsertStat :exec\nINSERT INTO stats VALUES ($1) ON CONFLICT DO NOTHING"
		cte    = "WITH moved AS (SELECT 1 AS id) INSERT INTO stats (id) SELECT id FROM moved"
		purge  = "DELETE FROM stock"
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
				sent(requestExecute, "first", upsert), sent(requestSync, "", ""),
				answered(msgCommandComplete, "INSERT 0 1"), answered(msgReadyForQuery, ""),
				sent(requestExecute, "again", upsert), sent(requestSync, "", ""),
				answered(msgCommandComplete, "INSERT 0 0"), answered(msgReadyForQuery, ""),
			),
			rejected: []string{"again"},
			answered: []string{"first"},
		},
		{
			name: "a ping is an empty query",
			steps: afterStartup(
				sent(requestSimple, "ping", "-- ping"),
				answered(msgEmptyQuery, ""), answered(msgReadyForQuery, ""),
			),
			rejected: []string{"ping"},
		},
		{
			name: "a read that found nothing still happened",
			steps: afterStartup(
				sent(requestExecute, "read", "SELECT qty FROM stock"), sent(requestSync, "", ""),
				answered(msgCommandComplete, "SELECT 0"), answered(msgReadyForQuery, ""),
			),
			answered: []string{"read"},
		},
		{
			// The pairing cannot be vouched for, so the statement stays counted.
			name: "a writing CTE is not matched to its tag",
			steps: afterStartup(
				sent(requestExecute, "cte", cte), sent(requestSync, "", ""),
				answered(msgCommandComplete, "INSERT 0 0"), answered(msgReadyForQuery, ""),
			),
			answered: []string{"cte"},
		},
		{
			name: "an error refuses its statement and everything skipped behind it",
			steps: afterStartup(
				sent(requestExecute, "claim", "INSERT INTO processed VALUES ($1)"),
				sent(requestExecute, "work", "UPDATE stock SET qty = qty - 1"),
				sent(requestSync, "", ""),
				answered(msgErrorResponse, ""), answered(msgReadyForQuery, ""),
			),
			rejected: []string{"claim", "work"},
		},
		{
			// Only the first statement can be matched to its tag; a second answer counts as a change.
			name: "a multi-statement query is not refused on a zero-row first answer",
			steps: afterStartup(
				sent(requestSimple, "multi", "DELETE FROM a; INSERT INTO b VALUES (1)"),
				answered(msgCommandComplete, "DELETE 0"), answered(msgCommandComplete, "INSERT 0 1"),
				answered(msgReadyForQuery, ""),
			),
			answered: []string{"multi"},
		},
		{
			name: "an answer nothing asked for stops the pairing, and nothing is refused after it",
			steps: afterStartup(
				answered(msgCommandComplete, "DELETE 0"),
				sent(requestExecute, "after", purge), sent(requestSync, "", ""),
				answered(msgCommandComplete, "DELETE 0"), answered(msgReadyForQuery, ""),
			),
			answered: []string{"after"},
		},
		{
			// Read as an answer, it would have broken the pairing before the first statement.
			name: "the ReadyForQuery that ends startup settles nothing",
			steps: afterStartup(
				sent(requestExecute, "first", purge), sent(requestSync, "", ""),
				answered(msgCommandComplete, "DELETE 0"), answered(msgReadyForQuery, ""),
			),
			rejected: []string{"first"},
		},
		{
			name: "a request queued before startup ended stops the pairing",
			steps: []step{
				sent(requestExecute, "early", purge), sent(requestSync, "", ""),
				answered(msgReadyForQuery, ""),
				answered(msgCommandComplete, "DELETE 0"), answered(msgReadyForQuery, ""),
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
