package pg

import (
	"strings"
	"sync"
	"unicode"
)

// Messages that decide what became of a statement. The frontend ones open a request, the backend
// ones settle it.
const (
	msgSync         = 'S'
	msgFunctionCall = 'F'

	msgCommandComplete = 'C'
	msgEmptyQuery      = 'I'
	msgErrorResponse   = 'E'
	msgPortalSuspended = 's'
	msgReadyForQuery   = 'Z'
)

// request is what a client asked the server to do, in the order it asked.
type request int

const (
	// requestSimple is a Query message: every statement in it is answered, then ReadyForQuery.
	requestSimple request = iota
	// requestExecute is an Execute message, answered by exactly one completion, error or suspension.
	requestExecute
	// requestSync ends an extended-protocol batch and is answered by ReadyForQuery.
	requestSync
	// requestCall is a FunctionCall, answered by ReadyForQuery. It is never an effect.
	requestCall
)

// awaited is one request still owed an answer.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type awaited struct {
	// token names the effect this request was recorded as. Empty when it was not recorded, in which
	// case it still holds its place in the queue, because the server answers it all the same.
	token string
	// verb is the statement's leading keyword, which its completion tag has to repeat.
	verb string
	kind request
	// answers counts the completions a simple query has had, and nothingChanged says whether every one
	// of them said so.
	answers        int
	nothingChanged bool
	failed         bool
}

// completions pairs each statement with the server's answer to it, to learn whether it changed
// anything.
//
// A statement the database says changed nothing is not an effect, for the reason a refused bus
// operation is not: an ON CONFLICT DO NOTHING insert re-sent under redelivery, or an update whose
// guard matched no row, is the handler being idempotent — measured on a real target, where exactly
// that insert was reported as a double write. The answer is read to learn WHETHER the statement
// changed something, never to interpret it.
//
// Postgres answers strictly in request order on a connection, so a queue is enough. Anything that
// breaks the pairing stops it for the rest of the connection: every statement is then answered as
// having changed something, because hiding a write that happened would be a false clean.
type completions struct {
	sink  Sink
	queue []awaited
	// started is set by the ReadyForQuery that ends connection startup. It answers no request, and a
	// client may not send one before it, so it is the one ReadyForQuery with nothing to settle.
	started bool
	broken  bool
	mu      sync.Mutex
}

// expect queues a request the client has just sent. It runs before the request is forwarded, so its
// answer can never arrive first.
func (c *completions) expect(kind request, token, sql string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.broken {
		c.answer(token)

		return
	}

	c.queue = append(c.queue, awaited{token: token, verb: leadingVerb(sql), kind: kind, nothingChanged: true})
}

// settle reads one backend message.
func (c *completions) settle(msgType byte, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.broken {
		return
	}

	switch msgType {
	case msgCommandComplete:
		tag, _ := (&reader{buf: body}).cstring()
		c.complete(changedNothing(tag))
	case msgEmptyQuery:
		// An empty query has no verb and no tag; it did nothing by construction.
		c.complete(func(verb string) bool { return verb == "" })
	case msgPortalSuspended:
		c.complete(func(string) bool { return false })
	case msgErrorResponse:
		c.fail()
	case msgReadyForQuery:
		c.ready()
	default:
		// Rows, descriptions, notices and parse or bind acknowledgements settle nothing.
	}
}

// complete answers the oldest outstanding statement. nothing says, given that statement's verb,
// whether the answer means it changed nothing.
func (c *completions) complete(nothing func(verb string) bool) {
	if len(c.queue) == 0 {
		c.breakPairing()

		return
	}

	head := &c.queue[0]

	switch head.kind {
	case requestExecute:
		if nothing(head.verb) {
			c.refuse(head.token)
		} else {
			c.answer(head.token)
		}

		c.queue = c.queue[1:]
	case requestSimple:
		// A multi-statement query cannot be matched verb for verb, so only its first answer is judged
		// and a second one counts as a change. Over-reporting is the safe direction.
		head.nothingChanged = head.nothingChanged && head.answers == 0 && nothing(head.verb)
		head.answers++
	case requestSync, requestCall:
		c.breakPairing()
	default:
		// Every request kind is listed above.
	}
}

// fail settles the statement an error answered. It changed nothing, and in an extended batch nothing
// after it runs until the batch's Sync — those are settled when that Sync is.
func (c *completions) fail() {
	if len(c.queue) == 0 {
		return
	}

	head := &c.queue[0]

	switch head.kind {
	case requestExecute:
		c.refuse(head.token)
		c.queue = c.queue[1:]
	case requestSimple, requestCall:
		head.failed = true
	case requestSync:
		// An error in a Parse or Bind with no Execute behind it: the Sync ahead is still owed its
		// ReadyForQuery, and there is no statement to settle.
		return
	default:
		// Every request kind is listed above.
	}
}

// ready settles everything a ReadyForQuery closes: a simple query or function call, or an extended
// batch up to and including its Sync. An Execute still waiting there was skipped after an error.
func (c *completions) ready() {
	if !c.started {
		c.started = true

		// A request already waiting means the stream is not the one the protocol promises.
		if len(c.queue) > 0 {
			c.breakPairing()
		}

		return
	}

	if len(c.queue) == 0 {
		c.breakPairing()

		return
	}

	head := c.queue[0]

	switch head.kind {
	case requestSimple:
		if head.failed || (head.answers == 1 && head.nothingChanged) {
			c.refuse(head.token)
		} else {
			c.answer(head.token)
		}

		c.queue = c.queue[1:]
	case requestCall:
		c.queue = c.queue[1:]
	case requestExecute, requestSync:
		c.closeBatch()
	default:
		// Every request kind is listed above.
	}
}

// closeBatch settles an extended batch at its Sync. Any Execute still waiting never ran.
func (c *completions) closeBatch() {
	for len(c.queue) > 0 {
		next := c.queue[0]
		c.queue = c.queue[1:]

		if next.kind == requestSync {
			return
		}

		c.refuse(next.token)
	}

	c.breakPairing()
}

// refuse marks a recorded statement as having changed nothing.
func (c *completions) refuse(token string) {
	if token != "" {
		c.sink.Reject(token)
	}
}

// answer releases a recorded statement that changed something.
func (c *completions) answer(token string) {
	if token != "" {
		c.sink.Answered(token)
	}
}

// breakPairing gives up on the connection: an answer arrived that no request accounts for, so every
// later pairing would be a guess. What is still waiting is treated as having changed something.
func (c *completions) breakPairing() {
	c.broken = true

	for _, waiting := range c.queue {
		c.answer(waiting.token)
	}

	c.queue = nil
}

// changedNothing reads a completion tag: INSERT, UPDATE, DELETE and MERGE report how many rows they
// touched, and a zero there is the statement saying it changed nothing. The tag must name the
// statement's own verb — anything else is a pairing Stutter cannot vouch for, and a CTE that writes
// is one of those, so it is left counted.
func changedNothing(tag string) func(verb string) bool {
	return func(verb string) bool {
		fields := strings.Fields(tag)
		if len(fields) < 2 || fields[0] != verb {
			return false
		}

		switch verb {
		case "INSERT", "UPDATE", "DELETE", "MERGE":
			return fields[len(fields)-1] == "0"
		default:
			return false
		}
	}
}

// leadingVerb is a statement's first keyword, upper-cased, after any comments. Query builders prefix
// statements with a comment naming them, so skipping comments is the common case, not an edge.
func leadingVerb(sql string) string {
	rest := sql

	for {
		rest = strings.TrimLeftFunc(rest, unicode.IsSpace)

		switch {
		case strings.HasPrefix(rest, "--"):
			_, after, found := strings.Cut(rest, "\n")
			if !found {
				return ""
			}

			rest = after
		case strings.HasPrefix(rest, "/*"):
			_, after, found := strings.Cut(rest, "*/")
			if !found {
				return ""
			}

			rest = after
		default:
			end := strings.IndexFunc(rest, func(r rune) bool { return !unicode.IsLetter(r) })
			if end < 0 {
				end = len(rest)
			}

			return strings.ToUpper(rest[:end])
		}
	}
}
