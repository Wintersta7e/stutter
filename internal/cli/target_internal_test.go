package cli

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/replay"
)

// testService is the service under test, and the consumer, of every fixture here.
const testService = "orders"

// TestAnExitIsReadFromTheStateBeforeTheKill: how the service ended is what the engine said before
// Stutter stopped it, and only a service that stopped by itself exited.
func TestAnExitIsReadFromTheStateBeforeTheKill(t *testing.T) {
	t.Parallel()

	const log = "logs/target-3.log"

	cases := []struct {
		name     string
		state    provision.State
		want     replay.Exit
		byItself bool
	}{
		{"exited with code 3", provision.State{ExitCode: 3, Log: log}, replay.Exit{
			Code: 3, Log: log, Exited: true,
		}, true},
		{"killed for memory", provision.State{ExitCode: 137, OOMKilled: true}, replay.Exit{
			Code: 137, OOMKilled: true, Exited: true,
		}, true},
		{"restarted twice", provision.State{RestartCount: 2, Running: true}, replay.Exit{Restarts: 2}, false},
		{"stopped by Stutter", provision.State{ExitCode: 137, Log: log}, replay.Exit{Code: 137, Log: log}, false},
	}

	for _, testCase := range cases {
		got := exitFrom(testCase.state, testCase.byItself)
		if got != testCase.want {
			t.Errorf("%s: exitFrom = %+v, want %+v", testCase.name, got, testCase.want)
		}

		if got.After != 0 {
			t.Errorf("%s: After = %d, want 0 — the harness reads it from its own clock", testCase.name, got.After)
		}
	}
}

// TestTheTargetJoinsItsNetworkOnlyWhenStarted: a created container is not a network member until it
// starts, so its membership is checked after the start; the relays are checked before anything is
// created, and the audit before anything runs.
func TestTheTargetJoinsItsNetworkOnlyWhenStarted(t *testing.T) {
	t.Parallel()

	steps := (&target{}).startSteps(rules.KindTarget, &started{})

	names := make([]string, 0, len(steps))
	for _, each := range steps {
		names = append(names, each.name)
	}

	t.Logf("order=%s", strings.Join(names, ","))

	at := func(name string) int {
		index := slices.Index(names, name)
		if index < 0 {
			t.Fatalf("no %q step in %v", name, names)
		}

		return index
	}

	const create = "create"

	for _, rule := range []struct{ before, after string }{
		{"live", create},
		{"spec", create},
		{create, "audit"},
		{"audit", "start"},
		{"start", "check-target"},
	} {
		if at(rule.before) >= at(rule.after) {
			t.Errorf("%s runs at %d, after %s at %d", rule.before, at(rule.before), rule.after, at(rule.after))
		}
	}
}

// TestEveryTargetTrustsTheCheckCA: a target is told where the check's CA is and has it bound there,
// read-only, from the check's own file.
func TestEveryTargetTrustsTheCheckCA(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "compose.yaml")
	if err := os.WriteFile(file, []byte("services: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	run := func(_ context.Context, _ string, _ []string, read compose.ConfigRead) ([]byte, int, error) {
		if read.Environment {
			return nil, 0, nil
		}

		return []byte(`{"name":"shop","services":{"orders":{"image":"orders:1"}}}`), 0, nil
	}

	model, err := compose.Parse(t.Context(), run, compose.Inputs{Service: testService, Files: []string{file}})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	const ca = "/tmp/stutter-check/ca.pem"

	spec, err := (&target{model: model, service: testService, ca: ca}).spec()
	if err != nil {
		t.Fatalf("spec: %v", err)
	}

	bound := slices.ContainsFunc(spec.Mounts, func(m compose.Mount) bool {
		return m.Kind == compose.MountBind && m.Source == ca && m.Target == compose.CAMountPath && m.ReadOnly
	})
	if !bound {
		t.Errorf("no read-only bind of %s at %s in %+v", ca, compose.CAMountPath, spec.Mounts)
	}

	for _, name := range compose.CAVariables() {
		if spec.Env[name] != compose.CAMountPath {
			t.Errorf("%s = %q, want %s", name, spec.Env[name], compose.CAMountPath)
		}
	}
}
