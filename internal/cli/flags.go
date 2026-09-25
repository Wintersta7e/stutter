package cli

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	httpproxy "github.com/Wintersta7e/stutter/internal/proxy/http"
)

// The commands that take flags.
const (
	commandCheck = "check"
	commandGate  = "gate"
	commandClean = "clean"
)

// flagCorpus names the corpus directory's flag.
const flagCorpus = "corpus"

var (
	// errGivenTwice means a flag that takes one value was given more than once: Go's flag package keeps
	// the last silently.
	errGivenTwice = errors.New("given more than once")
	// errNotPositive means a duration flag was zero or negative.
	errNotPositive = errors.New("must be a positive duration")
	// errNegative means --max-runs was negative.
	errNegative = errors.New("must not be negative")
	// errPositional means an argument was not a flag: Go's flag package stops at the first one and
	// silently ignores the rest.
	errPositional = errors.New("takes no positional argument")
	// errNoTarget means neither path was named.
	errNoTarget = errors.New("nothing to check")
	// errBothTargets means both paths were named.
	errBothTargets = errors.New("--postgres and --compose name two different checks")
	// errComposeOnly means a flag only the compose path reads was given without --compose.
	errComposeOnly = errors.New("only a --compose check takes this flag")
	// errMissing means --compose was given without a name it needs.
	errMissing = errors.New("--compose also needs")
	// errUnreadable means an input path is not what its flag needs.
	errUnreadable = errors.New("cannot be read")
)

// composeOnly are the flags only the compose path reads.
var composeOnly = []string{ //nolint:gochecknoglobals // a fixed table, not mutable state.
	"service", "stream", flagCorpus, "profile", "consumer", "startup", "drain", "keep", "routes",
}

// timingFlags are the flags that set a wait, in the order the header names them.
var timingFlags = []string{"startup", "quiesce", "drain"} //nolint:gochecknoglobals // a fixed table.

// onceString is a string flag that may be given once.
type onceString struct {
	value string
	set   bool
}

func (o *onceString) String() string {
	return o.value
}

// Set takes the flag's one value.
func (o *onceString) Set(value string) error {
	if o.set {
		return errGivenTwice
	}

	o.value, o.set = value, true

	return nil
}

// repeated is a flag that may be given any number of times, its values kept in the order given.
type repeated []string

func (r *repeated) String() string {
	return strings.Join(*r, ",")
}

// Set appends one value.
func (r *repeated) Set(value string) error {
	*r = append(*r, value)

	return nil
}

// onceDuration is a positive duration flag that may be given once.
type onceDuration struct {
	value time.Duration
	set   bool
}

func (o *onceDuration) String() string {
	if !o.set {
		return ""
	}

	return o.value.String()
}

// Set parses the flag's one value.
func (o *onceDuration) Set(value string) error {
	if o.set {
		return errGivenTwice
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return err //nolint:wrapcheck // the flag package names the flag and the value.
	}

	if parsed <= 0 {
		return errNotPositive
	}

	o.value, o.set = parsed, true

	return nil
}

// onceCount is a non-negative count flag that may be given once, starting from its default.
type onceCount struct {
	value int
	set   bool
}

func (o *onceCount) String() string {
	return strconv.Itoa(o.value)
}

// Set parses the flag's one value.
func (o *onceCount) Set(value string) error {
	if o.set {
		return errGivenTwice
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return err //nolint:wrapcheck // the flag package names the flag and the value.
	}

	if parsed < 0 {
		return errNegative
	}

	o.value, o.set = parsed, true

	return nil
}

// settings are check's and gate's flags, as parsed.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type settings struct {
	// given names every flag that appeared on the command line.
	given     map[string]bool
	compose   repeated
	profiles  repeated
	consumers repeated
	postgres  onceString
	service   onceString
	stream    onceString
	corpus    onceString
	routes    onceString
	quiesce   onceDuration
	startup   onceDuration
	drain     onceDuration
	maxRuns   onceCount
	keep      bool
}

// newFlagSet is check's or gate's flag set: gate injects nothing, so it has no run budget.
func newFlagSet(command string, parsed *settings, stderr io.Writer) *flag.FlagSet {
	set := flag.NewFlagSet(command, flag.ContinueOnError)
	set.SetOutput(stderr)
	set.Usage = func() {}

	set.Var(&parsed.compose, "compose", "a compose file; repeat for several, merged in the order given")
	set.Var(&parsed.service, "service", "the compose service under test")
	set.Var(&parsed.stream, "stream", "the stream the service consumes the corpus from")
	set.Var(&parsed.corpus, flagCorpus, "the corpus directory: one file per message")
	set.Var(&parsed.profiles, "profile", "a compose profile to activate; repeatable")
	set.Var(&parsed.consumers, "consumer", "a discovered consumer to check; repeatable")
	set.Var(&parsed.routes, "routes", "a JSON file of stub replies for external hosts")
	set.Var(&parsed.quiesce, "quiesce", "how long a message's window stays open after its handler returns")
	set.Var(&parsed.startup, "startup", "how long the service may take to create its consumer")
	set.Var(&parsed.drain, "drain", "how long a run waits in silence for messages still owed")
	set.BoolVar(&parsed.keep, "keep", false, "keep the check's resources for stutter clean")
	set.Var(&parsed.postgres, "postgres", "the reference path's scratch Postgres")

	if command == commandCheck {
		set.Var(&parsed.maxRuns, "max-runs", "each consumer check's cap on faulted runs; 0 for every legal pair")
	}

	return set
}

// parse reads check's or gate's arguments and refuses any the check cannot run, before anything
// reaches the engine.
func parse(args []string, stderr io.Writer, command string) (settings, error) {
	parsed := settings{maxRuns: onceCount{value: defaultMaxRuns}, given: map[string]bool{}}

	set := newFlagSet(command, &parsed, stderr)
	if err := set.Parse(args); err != nil {
		if command == commandGate && strings.Contains(err.Error(), "-max-runs") {
			return settings{}, fmt.Errorf("%w; %s", err, gateInjectsNothing)
		}

		return settings{}, err //nolint:wrapcheck // the flag package names the flag.
	}

	if set.NArg() > 0 {
		return settings{}, fmt.Errorf("%s %w: %s", command, errPositional, strings.Join(set.Args(), " "))
	}

	set.Visit(func(f *flag.Flag) { parsed.given[f.Name] = true })

	return parsed, parsed.refuse()
}

// refuse refuses a command line naming no check, both checks, a flag the named check does not read,
// or an input path that is not what its flag needs.
func (s settings) refuse() error {
	composing := len(s.compose) > 0

	switch {
	case !composing && !s.postgres.set:
		return fmt.Errorf("%w: %s", errNoTarget, nothingToCheck)
	case composing && s.postgres.set:
		return errBothTargets
	case !composing:
		for _, name := range composeOnly {
			if s.given[name] {
				return fmt.Errorf("--%s: %w", name, errComposeOnly)
			}
		}

		return nil
	default:
		return s.refuseCompose()
	}
}

// refuseCompose refuses a compose check missing a name it needs, or one whose files are not readable.
func (s settings) refuseCompose() error {
	var missing []string

	for name, value := range map[string]onceString{"service": s.service, "stream": s.stream, flagCorpus: s.corpus} {
		if !value.set {
			missing = append(missing, "--"+name)
		}
	}

	if len(missing) > 0 {
		slices.Sort(missing)

		return fmt.Errorf("%w %s", errMissing, strings.Join(missing, ", "))
	}

	for _, file := range s.compose {
		if err := regularFile(file); err != nil {
			return fmt.Errorf("--compose %s: %w", file, err)
		}
	}

	if s.routes.set {
		if err := regularFile(s.routes.value); err != nil {
			return fmt.Errorf("--routes %s: %w", s.routes.value, err)
		}
	}

	if info, err := os.Stat(s.corpus.value); err != nil || !info.IsDir() {
		return fmt.Errorf("--corpus %s: %w: not a directory", s.corpus.value, errUnreadable)
	}

	return nil
}

// regularFile refuses a path that is not a readable regular file.
func regularFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%w: %w", errUnreadable, err)
	}

	info, err := file.Stat()
	closeErr := file.Close()

	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("%w: not a regular file", errUnreadable)
	}

	return closeErr //nolint:wrapcheck // named with its flag by the caller.
}

// composeRun is the compose check the command line asked for: every path made absolute, the routes
// file decoded — an invalid one is refused here, before anything reaches the engine.
func (s settings) composeRun(gatesOnly bool) (composeRun, error) {
	run := composeRun{
		service: s.service.value, stream: s.stream.value, corpus: s.corpus.value, routes: s.routes.value,
		compose: s.compose, profiles: s.profiles, consumers: s.consumers, startup: s.startup.value,
		quiesce: s.quiesce.value, drain: s.drain.value, maxRuns: s.maxRuns.value, keep: s.keep, gatesOnly: gatesOnly,
	}

	for _, file := range s.compose {
		absolute, err := filepath.Abs(file)
		if err != nil {
			return composeRun{}, fmt.Errorf("--compose %s: %w", file, err)
		}

		run.files = append(run.files, absolute)
	}

	for _, name := range timingFlags {
		if s.given[name] {
			run.timingFlags = append(run.timingFlags, "--"+name)
		}
	}

	if !s.routes.set {
		return run, nil
	}

	routes, err := os.ReadFile(s.routes.value)
	if err != nil {
		return composeRun{}, fmt.Errorf("--routes %s: %w", s.routes.value, err)
	}

	if run.httpDefault, run.httpRoutes, err = httpproxy.DecodeRoutes(bytes.NewReader(routes)); err != nil {
		return composeRun{}, fmt.Errorf("--routes %s: %w", s.routes.value, err)
	}

	return run, nil
}
