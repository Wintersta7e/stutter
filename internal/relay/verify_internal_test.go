package relay

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"
)

// verifyListener is the host's verification listener as a test plays it: it reads the probe's
// preamble and answers with ack, or never answers when ack is nil.
func verifyListener(t *testing.T, token Token, ack []byte) netip.AddrPort {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			t.Cleanup(func() { _ = conn.Close() })

			probe, probeErr := Accept(conn, token)
			if probeErr != nil || probe.DestinationPort() != 0 || ack == nil {
				continue
			}

			_, _ = probe.Write(ack) //nolint:errcheck // the verifier's read judges it.
		}
	}()

	return netip.MustParseAddrPort(listener.Addr().String())
}

func verifyArgs(token Token, target string, port uint16, dial time.Duration) []string {
	return Verify{Target: target, Dial: dial, Token: token, Port: port}.Args()
}

func TestVerifyReportsTheAddressThatAnswered(t *testing.T) {
	t.Parallel()

	token := testToken(t)
	host := verifyListener(t, token, []byte(magic))

	run := runSystem(t, system{}, verifyArgs(token, "127.0.0.1", host.Port(), time.Second))

	line, printed := run.line(t)
	if !printed || line != Verified+" 127.0.0.1" {
		t.Errorf("stdout = %q, want %q (stderr %q)", line, Verified+" 127.0.0.1", run.stderr)
	}

	if code := run.wait(t); code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
}

// TestVerifyGivesUpAtItsDialDeadline bounds the verifier by its own deadline, so a host that accepts
// and never answers is reported as the verification failing rather than as the verifier's container
// outliving its wait.
func TestVerifyGivesUpAtItsDialDeadline(t *testing.T) {
	t.Parallel()

	const dial = 300 * time.Millisecond

	token := testToken(t)
	host := verifyListener(t, token, nil)

	began := time.Now()
	run := runSystem(t, system{}, verifyArgs(token, "127.0.0.1", host.Port(), dial))

	select {
	case <-run.done:
	case <-time.After(2 * time.Second):
		t.Fatal("verify still running")
	}

	elapsed := time.Since(began)
	stderr := run.stderr.String()

	if run.code != exitFailure || elapsed >= dial+200*time.Millisecond {
		t.Errorf("exit %d after %s, want %d within %s", run.code, elapsed, exitFailure, dial+200*time.Millisecond)
	}

	if !strings.Contains(stderr, host.String()) || !strings.Contains(stderr, "timed out") {
		t.Errorf("stderr = %q, want it to name %s and timed out", stderr, host)
	}
}

// TestVerifyWaitsForAListenerThatOpensLate keeps verifying until the deadline while the host refuses:
// measured on Docker Desktop, a listener the host has just opened is refused through
// host.docker.internal for 0.73 to 0.82 s, until WSL's localhost forwarding notices it.
func TestVerifyWaitsForAListenerThatOpensLate(t *testing.T) {
	t.Parallel()

	token := testToken(t)
	late := loopback(unusedPort(t))

	run := runSystem(t, system{}, verifyArgs(token, "127.0.0.1", late.Port(), 3*time.Second))

	time.Sleep(500 * time.Millisecond)

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", late.String())
	if err != nil {
		t.Fatalf("listen late: %v", err)
	}

	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}

		defer func() { _ = conn.Close() }()

		if probe, probeErr := Accept(conn, token); probeErr == nil {
			_ = WriteAck(probe) //nolint:errcheck // the verifier's exit judges it.
		}
	}()

	if code := run.wait(t); code != 0 {
		t.Errorf("a listener that opened 500ms late: exit = %d, want 0 (stderr %q)", code, run.stderr)
	}
}

// TestVerifyNamesEachFailure keeps every failure distinguishable: the verification's error is the one
// line an unreachable host leaves for its user.
func TestVerifyNamesEachFailure(t *testing.T) {
	t.Parallel()

	token := testToken(t)
	refused := loopback(unusedPort(t))
	wrongAck := verifyListener(t, token, []byte("NOPE"))

	for name, host := range map[string]netip.AddrPort{"refused": refused, "wrong ack": wrongAck} {
		run := runSystem(t, system{}, verifyArgs(token, "127.0.0.1", host.Port(), time.Second))

		if code := run.wait(t); code != exitFailure {
			t.Errorf("%s: exit = %d, want %d", name, code, exitFailure)
		}

		if stderr := run.stderr.String(); !strings.Contains(stderr, name) || !strings.Contains(stderr, host.String()) {
			t.Errorf("%s: stderr = %q, want it to name %s and %q", name, stderr, host, name)
		}
	}
}

// TestVerifyNeedsExactlyOneAnswer refuses to guess which of several addresses is the host.
func TestVerifyNeedsExactlyOneAnswer(t *testing.T) {
	t.Parallel()

	token := testToken(t)

	for _, answers := range [][]netip.Addr{
		nil,
		{netip.MustParseAddr("192.168.65.254"), netip.MustParseAddr("192.168.65.253")},
	} {
		sys := system{lookup: func(context.Context, string) ([]netip.Addr, error) { return answers, nil }}
		run := runSystem(t, sys, verifyArgs(token, hostAlias, 40000, time.Second))

		if code := run.wait(t); code != exitFailure {
			t.Errorf("%d answers: exit = %d, want %d", len(answers), code, exitFailure)
		}

		stderr := run.stderr.String()
		if !strings.Contains(stderr, strconv.Itoa(len(answers))+" addresses") {
			t.Errorf("%d answers: stderr = %q, want it to count them", len(answers), stderr)
		}

		for _, answer := range answers {
			if !strings.Contains(stderr, answer.String()) {
				t.Errorf("%d answers: stderr = %q does not name %s", len(answers), stderr, answer)
			}
		}
	}
}
