package cli

import (
	"bytes"
	"flag"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// commandFlags are the flags a command's real flag set defines.
func commandFlags(command string) []string {
	var names []string

	set := newFlagSet(command, &settings{}, io.Discard)
	if command == commandClean {
		set = newCleanFlagSet(&cleanSettings{}, io.Discard)
	}

	set.VisitAll(func(f *flag.Flag) { names = append(names, f.Name) })
	slices.Sort(names)

	return names
}

// TestEveryFlagIsInTheTable: each command defines exactly its table's flags.
func TestEveryFlagIsInTheTable(t *testing.T) {
	t.Parallel()

	gate := []string{
		"compose", "consumer", "corpus", "drain", "keep", "postgres", "profile", "quiesce", "routes", "service",
		"startup", "stream",
	}
	check := append(slices.Clone(gate), "max-runs")
	slices.Sort(check)

	want := map[string][]string{commandCheck: check, commandGate: gate, commandClean: {commandCheck, "dry-run"}}

	counts := make([]string, 0, len(want))

	for _, command := range []string{commandCheck, commandGate, commandClean} {
		got := commandFlags(command)
		counts = append(counts, command+"="+strconv.Itoa(len(got)))

		if !slices.Equal(got, want[command]) {
			t.Errorf("%s flags = %v, want %v", command, got, want[command])
		}

		if len(got) == 0 {
			t.Errorf("%s defines no flag", command)
		}
	}

	t.Log(strings.Join(counts, " "))
}

// flagCompose is the compose file's flag, as typed.
const (
	flagCompose      = "--compose"
	flagCorpusTyped  = "--corpus"
	flagServiceTyped = "--service"
	flagStreamTyped  = "--stream"
	streamOrders     = "ORDERS"
)

// composeProject is a compose file, a corpus directory and a valid routes file, all readable.
type composeProject struct {
	file   string
	corpus string
	routes string
}

func newComposeProject(t *testing.T) composeProject {
	t.Helper()

	dir := t.TempDir()
	project := composeProject{
		file: filepath.Join(dir, "compose.yaml"), corpus: filepath.Join(dir, "corpus"),
		routes: filepath.Join(dir, "routes.json"),
	}

	for path, content := range map[string]string{project.file: "services: {}\n", project.routes: "{}\n"} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.Mkdir(project.corpus, 0o700); err != nil {
		t.Fatal(err)
	}

	return project
}

func (p composeProject) args(extra ...string) []string {
	return append([]string{
		commandCheck,
		flagCompose,
		p.file,
		flagServiceTyped,
		testService,
		flagStreamTyped,
		streamOrders,
		flagCorpusTyped,
		p.corpus,
	}, extra...)
}

// TestEveryArgumentRefusalExitsBeforeDocker: every argument the command line can refuse is refused,
// naming the flag, before anything reaches the engine.
func TestEveryArgumentRefusalExitsBeforeDocker(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent.sock")

	project := newComposeProject(t)
	missing := filepath.Join(t.TempDir(), "absent.yaml")

	cases := []struct {
		row   string
		names string
		args  []string
	}{
		{"R1", "--postgres", []string{commandCheck}},
		{"R2", flagCompose, append(project.args(), "--postgres", "postgres://u@127.0.0.1:1/db")},
		{
			"R3",
			flagStreamTyped,
			[]string{commandCheck, "--postgres", "postgres://u@127.0.0.1:1/db", flagStreamTyped, streamOrders},
		},
		{
			"R4",
			flagCorpusTyped,
			[]string{
				commandCheck,
				flagCompose,
				project.file,
				flagServiceTyped,
				testService,
				flagStreamTyped,
				streamOrders,
			},
		},
		{"R5", "-service", append(project.args(), flagServiceTyped, "billing")},
		{"R6", "b.yaml", append(project.args(), "b.yaml")},
		{"R7 negative", "-max-runs", append(project.args(), "--max-runs", "-1")},
		{"R7 gate", "-max-runs", append([]string{commandGate}, append(project.args()[1:], "--max-runs", "1")...)},
		{"R8 unparsable", "-drain", append(project.args(), "--drain", "soon")},
		{"R8 not positive", "-startup", append(project.args(), "--startup", "0s")},
		{
			"R9 compose",
			flagCompose,
			[]string{
				commandCheck,
				flagCompose,
				missing,
				flagServiceTyped,
				testService,
				flagStreamTyped,
				streamOrders,
				flagCorpusTyped,
				project.corpus,
			},
		},
		{"R9 routes", "--routes", append(project.args(), "--routes", missing)},
		{
			"R9 corpus",
			flagCorpusTyped,
			[]string{
				commandCheck,
				flagCompose,
				project.file,
				flagServiceTyped,
				testService,
				flagStreamTyped,
				streamOrders,
				flagCorpusTyped,
				project.file,
			},
		},
		{"R10 flag", "-nonsense", append(project.args(), "--nonsense")},
		{"R10 command", "wibble", []string{"wibble"}},
	}

	rows := map[string]bool{}

	for _, testCase := range cases {
		var stdout, stderr bytes.Buffer

		code := Run(t.Context(), testCase.args, &stdout, &stderr)
		if code != exitUsage {
			t.Errorf("%s: exit = %d, want %d (stderr: %s)", testCase.row, code, exitUsage, stderr.String())
		}

		if !strings.Contains(stderr.String(), testCase.names) {
			t.Errorf("%s: stderr does not name %s:\n%s", testCase.row, testCase.names, stderr.String())
		}

		reached := slices.ContainsFunc([]string{"docker", "engine", "Cannot connect"}, func(word string) bool {
			return strings.Contains(stderr.String(), word)
		})
		if reached || stdout.Len() > 0 {
			t.Errorf("%s: the refusal reached the engine:\nstdout: %s\nstderr: %s", testCase.row, stdout.String(),
				stderr.String())
		}

		rows[strings.Fields(testCase.row)[0]] = true
	}

	t.Logf("cases=%d rows=%d", len(cases), len(rows))

	if len(rows) != 10 {
		t.Errorf("%d rows covered, want R1–R10", len(rows))
	}
}

// TestAnInvalidRoutesFileNamesTheFileAndPath: a routes file the stub would not honour is refused
// before any engine call, naming the file and the JSON path.
func TestAnInvalidRoutesFileNamesTheFileAndPath(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent.sock")

	project := newComposeProject(t)

	invalid := `{"routes": [{"path": "/a"}, {"path": "/b", "json": 1, "text": "one"}]}`
	if err := os.WriteFile(project.routes, []byte(invalid), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer

	code := Run(t.Context(), project.args("--routes", project.routes), &stdout, &stderr)
	if code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}

	for _, want := range []string{project.routes, "routes[1]"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr does not name %s:\n%s", want, stderr.String())
		}
	}

	if stdout.Len() > 0 {
		t.Errorf("the refusal rendered a report:\n%s", stdout.String())
	}
}
