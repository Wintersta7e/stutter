package corpus_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/corpus"
)

// maxMessage is the embedded server's max_payload, which a message's header block and payload share.
const maxMessage = 1 << 20

// File names the grammar cases share.
const (
	fileA1        = "1.a.json"
	fileA2        = "2.a.json"
	sidecarA1     = "1.a.headers"
	sidecarA2     = "2.a.headers"
	ordersJSON    = "1.orders.json"
	ordersBin     = "1.orders.bin"
	ordersHeaders = "1.orders.headers"
)

// entry is one directory entry a grammar case lays down: a file with content, a subdirectory, or a
// symbolic link to another entry.
type entry struct {
	content string
	linkTo  string
	dir     bool
}

// grammarCase is one clause of the corpus directory grammar, valid or not.
type grammarCase struct {
	check   func(t *testing.T, loaded corpus.Loaded)
	entries map[string]entry
	name    string
	// names is the offending file an error must name; empty for a valid case.
	names []string
}

// TestLoadDirGrammar is the published corpus format, one case per clause: users hand-write this
// directory, so every refusal names the file it is about.
func TestLoadDirGrammar(t *testing.T) {
	t.Parallel()

	cases := slices.Concat(validGrammarCases(), invalidGrammarCases())

	t.Logf("%d cases", len(cases))

	if len(cases) == 0 {
		t.Fatal("no grammar cases")
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			loaded, err := corpus.LoadDir(layDown(t, testCase.entries))

			if len(testCase.names) == 0 {
				if err != nil {
					t.Fatalf("LoadDir() error = %v", err)
				}

				testCase.check(t, loaded)

				return
			}

			if !errors.Is(err, corpus.ErrInvalidCorpus) {
				t.Fatalf("LoadDir() error = %v, want ErrInvalidCorpus", err)
			}

			for _, name := range testCase.names {
				if !strings.Contains(err.Error(), name) {
					t.Errorf("LoadDir() error = %v, want it to name %s", err, name)
				}
			}
		})
	}
}

func validGrammarCases() []grammarCase {
	return []grammarCase{
		{
			name:    "2 before 10",
			entries: files("10.orders.b.json", "2.orders.a.json"),
			check: func(t *testing.T, loaded corpus.Loaded) {
				t.Helper()
				wantFiles(t, loaded, "2.orders.a.json", "10.orders.b.json")
				wantSubjects(t, loaded, "orders.a", "orders.b")

				for at, message := range loaded.Messages {
					if message.Seq != uint64(at)+1 {
						t.Errorf("Messages[%d].Seq = %d, want its rank %d", at, message.Seq, at+1)
					}
				}
			},
		},
		{
			name:    "zero padding is optional",
			entries: files("2.orders.txt", "001.orders.bin"),
			check: func(t *testing.T, loaded corpus.Loaded) {
				t.Helper()
				wantFiles(t, loaded, "001.orders.bin", "2.orders.txt")
			},
		},
		{
			name:    "percent-encoded slash",
			entries: files("1.orders%2Fcreated.json"),
			check: func(t *testing.T, loaded corpus.Loaded) {
				t.Helper()
				wantSubjects(t, loaded, "orders/created")
			},
		},
		{
			name: "dotfiles skipped and counted",
			entries: map[string]entry{
				".notes":         {content: "not a message"},
				".cache":         {dir: true},
				ordersJSON:       {content: "{}"},
				".1.orders.json": {content: "hidden"},
			},
			check: func(t *testing.T, loaded corpus.Loaded) {
				t.Helper()

				if loaded.Skipped != 3 || len(loaded.Messages) != 1 {
					t.Errorf("Skipped = %d, Messages = %d, want 3 and 1", loaded.Skipped, len(loaded.Messages))
				}
			},
		},
		{
			name: "sidecar joined",
			entries: map[string]entry{
				ordersJSON:    {content: "{}"},
				ordersHeaders: {content: "Idempotency-Key: k-1\r\n\r\nTrace:  two spaces\nIdempotency-Key: k-2\n"},
			},
			check: func(t *testing.T, loaded corpus.Loaded) {
				t.Helper()

				header := loaded.Messages[0].Header
				if loaded.Sidecars != 1 || !slices.Equal(header["Idempotency-Key"], []string{"k-1", "k-2"}) {
					t.Errorf("Sidecars = %d, Header = %q, want 1 and both keys in file order", loaded.Sidecars, header)
				}

				if got := header["Trace"]; !slices.Equal(got, []string{" two spaces"}) {
					t.Errorf("Trace = %q, want one space dropped after the colon", got)
				}
			},
		},
		{
			name:    "payload of exactly max_payload",
			entries: map[string]entry{ordersBin: {content: strings.Repeat("x", maxMessage)}},
			check: func(t *testing.T, loaded corpus.Loaded) {
				t.Helper()

				if got := len(loaded.Messages[0].Payload); got != maxMessage {
					t.Errorf("payload length = %d, want %d verbatim", got, maxMessage)
				}
			},
		},
	}
}

func invalidGrammarCases() []grammarCase {
	withSidecar := func(sidecar string) map[string]entry {
		return map[string]entry{fileA1: {content: "{}"}, sidecarA1: {content: sidecar}}
	}

	return []grammarCase{
		{name: "repeated order", entries: files(fileA1, "01.b.json"), names: []string{fileA1, "01.b.json"}},
		{name: "encoded token dot", entries: files("1.a%2Eb.json"), names: []string{"1.a%2Eb.json"}},
		{name: "malformed percent", entries: files("1.a%zz.json"), names: []string{"1.a%zz.json"}},
		{name: "dollar subject", entries: files("1.$SYS.x.json"), names: []string{"1.$SYS.x.json"}},
		{name: "invalid subject", entries: files("1.orders.*.json"), names: []string{"1.orders.*.json"}},
		{name: "missing extension", entries: files("00.orders.created"), names: []string{"00.orders.created"}},
		{name: "unknown extension", entries: files("1.orders.yaml"), names: []string{"1.orders.yaml"}},
		{name: "not an order", entries: files("first.orders.json"), names: []string{"first.orders.json"}},
		{
			name:    "orphan sidecar",
			entries: map[string]entry{fileA1: {content: "{}"}, sidecarA2: {content: "K: v\n"}},
			names:   []string{sidecarA2},
		},
		{
			name:    "malformed header line",
			entries: withSidecar("K: v\nno colon here\n"),
			names:   []string{sidecarA1, "line 2"},
		},
		{
			name:    "server-directing header",
			entries: withSidecar("Nats-Expected-Stream: X\n"),
			names:   []string{sidecarA1},
		},
		{
			name: "shared message id",
			entries: map[string]entry{
				fileA1: {content: "{}"}, sidecarA1: {content: "Nats-Msg-Id: same\n"},
				fileA2: {content: "{}"}, sidecarA2: {content: "Nats-Msg-Id: same\n"},
			},
			names: []string{sidecarA1, sidecarA2},
		},
		{
			name:    "subdirectory",
			entries: map[string]entry{fileA1: {content: "{}"}, fileA2: {dir: true}},
			names:   []string{fileA2},
		},
		{
			name:    "symbolic link",
			entries: map[string]entry{fileA1: {content: "{}"}, fileA2: {linkTo: fileA1}},
			names:   []string{fileA2},
		},
		{name: "empty corpus", entries: map[string]entry{".only": {content: "x"}}, names: []string{"no message"}},
		{
			name:    "one byte past max_payload",
			entries: map[string]entry{ordersBin: {content: strings.Repeat("x", maxMessage+1)}},
			names:   []string{ordersBin},
		},
		{
			name: "header block counts toward max_payload",
			entries: map[string]entry{
				ordersBin:     {content: strings.Repeat("x", maxMessage-10)},
				ordersHeaders: {content: "K: v\n"},
			},
			names: []string{ordersBin},
		},
	}
}

// TestLoadDirWritesNothing: the directory a user points Stutter at is theirs, so reading it changes
// no entry and adds none.
func TestLoadDirWritesNothing(t *testing.T) {
	t.Parallel()

	dir := layDown(t, map[string]entry{
		ordersJSON:     {content: `{"order_id":"ORD-1"}`},
		ordersHeaders:  {content: "Idempotency-Key: k-1\n"},
		"2.orders.txt": {content: "ORD-2"},
		".notes":       {content: "skipped"},
	})

	before := fingerprint(t, dir)

	t.Logf("%d entries", len(before))

	if len(before) == 0 {
		t.Fatal("no entries to compare")
	}

	if _, err := corpus.LoadDir(dir); err != nil {
		t.Fatalf("LoadDir() error = %v", err)
	}

	if after := fingerprint(t, dir); !slices.Equal(before, after) {
		t.Errorf("the corpus directory changed:\nbefore %q\nafter  %q", before, after)
	}
}

// TestAdmittedMatchesFilterSubjects: one matcher decides which messages a consumer's filter admits,
// wildcards included, keeping corpus order.
func TestAdmittedMatchesFilterSubjects(t *testing.T) {
	t.Parallel()

	messages := []corpus.Message{
		{Subject: orderCreated, Seq: 1},
		{Subject: "orders.eu.created", Seq: 2},
		{Subject: "payments.settled", Seq: 3},
		{Subject: orderCancelled, Seq: 4},
	}

	cases := []struct {
		name    string
		filters []string
		want    []uint64
	}{
		{name: "token wildcard", filters: []string{"orders.*"}, want: []uint64{1, 4}},
		{name: "tail wildcard", filters: []string{allOrders}, want: []uint64{1, 2, 4}},
		{name: "two filters", filters: []string{"payments.settled", orderCreated}, want: []uint64{1, 3}},
		{name: "no filter", want: []uint64{1, 2, 3, 4}},
	}

	for _, testCase := range cases {
		var got []uint64
		for _, message := range corpus.Admitted(messages, testCase.filters) {
			got = append(got, message.Seq)
		}

		if !slices.Equal(got, testCase.want) {
			t.Errorf("%s: Admitted() = %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

// layDown writes a corpus directory from entries and returns its path.
func layDown(t *testing.T, entries map[string]entry) string {
	t.Helper()

	dir := t.TempDir()

	for name, item := range entries {
		path := filepath.Join(dir, name)

		var err error

		switch {
		case item.dir:
			err = os.Mkdir(path, 0o700)
		case item.linkTo != "":
			err = os.Symlink(item.linkTo, path)
		default:
			err = os.WriteFile(path, []byte(item.content), 0o600)
		}

		if err != nil {
			t.Fatalf("lay down %s: %v", name, err)
		}
	}

	return dir
}

// files is one small JSON message per name.
func files(names ...string) map[string]entry {
	entries := make(map[string]entry, len(names))
	for _, name := range names {
		entries[name] = entry{content: "{}"}
	}

	return entries
}

func wantFiles(t *testing.T, loaded corpus.Loaded, want ...string) {
	t.Helper()

	if !slices.Equal(loaded.Files, want) {
		t.Errorf("Files = %q, want %q", loaded.Files, want)
	}
}

func wantSubjects(t *testing.T, loaded corpus.Loaded, want ...string) {
	t.Helper()

	subjects := make([]string, 0, len(loaded.Messages))
	for _, message := range loaded.Messages {
		subjects = append(subjects, message.Subject)
	}

	if !slices.Equal(subjects, want) {
		t.Errorf("subjects = %q, want %q", subjects, want)
	}
}

// fingerprint is every entry's name, size and SHA-256, in name order.
func fingerprint(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}

	prints := make([]string, 0, len(entries))

	for _, item := range entries {
		content, readErr := os.ReadFile(filepath.Join(dir, item.Name()))
		if readErr != nil {
			t.Fatalf("ReadFile(%s) error = %v", item.Name(), readErr)
		}

		sum := sha256.Sum256(content)
		prints = append(prints, fmt.Sprintf("%s %d %s", item.Name(), len(content), hex.EncodeToString(sum[:])))
	}

	return prints
}
