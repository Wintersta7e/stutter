package opaque_test

import (
	"bufio"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/effect"
	"github.com/Wintersta7e/stutter/internal/proxy/opaque"
)

const dependencyName = "cache"

type sink struct {
	observed []effect.Observation
	mu       sync.Mutex
}

func (s *sink) Record(observation effect.Observation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if observation.Kind != effect.KindOpaque {
		panic("an unparsed dependency produced an effect that claims to be parsed")
	}

	if observation.Raw != observation.Printable {
		panic("the readable form and the comparable form were expected to match")
	}

	s.observed = append(s.observed, observation)
}

func (s *sink) texts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	texts := make([]string, 0, len(s.observed))
	for _, observation := range s.observed {
		texts = append(texts, observation.Raw)
	}

	return texts
}

// startUpstream runs a dependency Stutter knows nothing about. handle owns one connection.
func startUpstream(t *testing.T, handle func(net.Conn)) string {
	t.Helper()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var served sync.WaitGroup

	served.Go(func() {
		for {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}

			served.Go(func() {
				defer func() { _ = connection.Close() }()

				handle(connection)
			})
		}
	})

	t.Cleanup(func() {
		_ = listener.Close()

		served.Wait()
	})

	return listener.Addr().String()
}

// answerLines replies to every line it is sent, which is the shape of most datastore protocols.
func answerLines(reply string) func(net.Conn) {
	return func(connection net.Conn) {
		reader := bufio.NewReader(connection)

		for {
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}

			if _, err := io.WriteString(connection, reply); err != nil {
				return
			}
		}
	}
}

func drain(connection net.Conn) {
	//nolint:errcheck // the drain ends when the peer goes away, which is the only outcome here.
	_, _ = io.Copy(io.Discard, connection)
}

func startProxy(t *testing.T, upstream string, current opaque.Sink) (*opaque.Proxy, <-chan error) {
	t.Helper()

	proxy, err := opaque.Listen(t.Context(), "127.0.0.1:0", dependencyName, upstream, current)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- proxy.Serve(t.Context()) }()

	return proxy, done
}

func closeProxy(t *testing.T, proxy *opaque.Proxy, done <-chan error) {
	t.Helper()

	if err := proxy.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not stop after Close")
	}
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()

	dialer := net.Dialer{Timeout: time.Second}

	connection, err := dialer.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	return connection
}

// TestEachAnsweredRequestIsOneEffect pins the framing rule. Without a protocol there is no message
// boundary to read, so the boundary is the upstream's answer.
func TestEachAnsweredRequestIsOneEffect(t *testing.T) {
	t.Parallel()

	current := &sink{}
	upstream := startUpstream(t, answerLines("+OK\n"))
	proxy, done := startProxy(t, upstream, current)

	client := dial(t, proxy.Addr())
	reader := bufio.NewReader(client)

	for _, request := range []string{"SET stock 12\n", "SET stock 9\n"} {
		if _, err := io.WriteString(client, request); err != nil {
			t.Fatal(err)
		}

		if _, err := reader.ReadString('\n'); err != nil {
			t.Fatal(err)
		}
	}

	_ = client.Close()

	closeProxy(t, proxy, done)

	want := []string{
		"dependency=cache payload=SET stock 12",
		"dependency=cache payload=SET stock 9",
	}
	if got := current.texts(); !slices.Equal(got, want) {
		t.Errorf("effects = %q, want %q", got, want)
	}
}

// TestUnansweredRequestSurvivesTheConnection covers fire-and-forget: a request nothing replies to is
// still a request, and losing it would report a handler as having done nothing.
func TestUnansweredRequestSurvivesTheConnection(t *testing.T) {
	t.Parallel()

	current := &sink{}
	proxy, done := startProxy(t, startUpstream(t, drain), current)

	client := dial(t, proxy.Addr())
	if _, err := io.WriteString(client, "METRIC stock.written 1\n"); err != nil {
		t.Fatal(err)
	}

	_ = client.Close()

	closeProxy(t, proxy, done)

	want := []string{"dependency=cache payload=METRIC stock.written 1"}
	if got := current.texts(); !slices.Equal(got, want) {
		t.Errorf("effects = %q, want %q", got, want)
	}
}

// TestGreetingRecordsNothing guards the emptiest false effect available: a dependency that speaks
// first must not close a request that was never made.
func TestGreetingRecordsNothing(t *testing.T) {
	t.Parallel()

	current := &sink{}
	upstream := startUpstream(t, func(connection net.Conn) {
		if _, err := io.WriteString(connection, "HELLO 1\n"); err != nil {
			return
		}

		answerLines("+OK\n")(connection)
	})
	proxy, done := startProxy(t, upstream, current)

	client := dial(t, proxy.Addr())
	reader := bufio.NewReader(client)

	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}

	if _, err := io.WriteString(client, "AUTH token\n"); err != nil {
		t.Fatal(err)
	}

	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}

	_ = client.Close()

	closeProxy(t, proxy, done)

	want := []string{"dependency=cache payload=AUTH token"}
	if got := current.texts(); !slices.Equal(got, want) {
		t.Errorf("effects = %q, want %q", got, want)
	}
}

func TestBinaryRequestIsHexEncodedOnOneLine(t *testing.T) {
	t.Parallel()

	current := &sink{}
	upstream := startUpstream(t, func(connection net.Conn) {
		if _, err := connection.Read(make([]byte, 8)); err != nil {
			return
		}

		//nolint:errcheck // the reply only has to reach the proxy, which frames the request.
		_, _ = connection.Write([]byte{0x00})
	})
	proxy, done := startProxy(t, upstream, current)

	client := dial(t, proxy.Addr())
	if _, err := client.Write([]byte{0x00, 0x01, 0xff, 0x7f}); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}

	_ = client.Close()

	closeProxy(t, proxy, done)

	want := []string{"dependency=cache payload=0x0001ff7f"}
	if got := current.texts(); !slices.Equal(got, want) {
		t.Errorf("effects = %q, want %q", got, want)
	}
}

func TestMultilineRequestStaysOneEffectLine(t *testing.T) {
	t.Parallel()

	current := &sink{}
	upstream := startUpstream(t, func(connection net.Conn) {
		if _, err := connection.Read(make([]byte, 64)); err != nil {
			return
		}

		//nolint:errcheck // the reply only has to reach the proxy, which frames the request.
		_, _ = io.WriteString(connection, "+OK\n")
	})
	proxy, done := startProxy(t, upstream, current)

	client := dial(t, proxy.Addr())
	if _, err := io.WriteString(client, "MULTI\r\nSET stock 12\r\nEXEC\r\n"); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Read(make([]byte, 4)); err != nil {
		t.Fatal(err)
	}

	_ = client.Close()

	closeProxy(t, proxy, done)

	got := current.texts()
	if len(got) != 1 {
		t.Fatalf("effects = %q, want one", got)
	}

	if strings.ContainsAny(got[0], "\r\n") {
		t.Errorf("effect spans more than one line: %q", got[0])
	}

	const want = "dependency=cache payload=MULTI SET stock 12 EXEC"
	if got[0] != want {
		t.Errorf("effect = %q, want %q", got[0], want)
	}
}

// TestOversizeRequestSplitsOnAnExactByteCount proves the split is byte-driven. Splitting where a
// TCP read happened to land would put a different boundary in every run and the determinism gate
// could never pass.
func TestOversizeRequestSplitsOnAnExactByteCount(t *testing.T) {
	t.Parallel()

	const (
		limit = 1 << 20
		extra = 10
	)

	current := &sink{}
	proxy, done := startProxy(t, startUpstream(t, drain), current)

	client := dial(t, proxy.Addr())
	if _, err := client.Write([]byte(strings.Repeat("a", limit+extra))); err != nil {
		t.Fatal(err)
	}

	_ = client.Close()

	closeProxy(t, proxy, done)

	got := current.texts()
	if len(got) != 2 {
		t.Fatalf("effects = %d, want the request split in two", len(got))
	}

	const prefix = "dependency=cache payload="
	if len(got[0]) != len(prefix)+limit {
		t.Errorf("first effect carried %d payload bytes, want %d", len(got[0])-len(prefix), limit)
	}

	if len(got[1]) != len(prefix)+extra {
		t.Errorf("second effect carried %d payload bytes, want %d", len(got[1])-len(prefix), extra)
	}
}

func TestListenRefusesAnUnusableConfiguration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		current  opaque.Sink
		name     string
		logical  string
		upstream string
	}{
		{name: "no sink", current: nil, logical: dependencyName, upstream: "127.0.0.1:1"},
		{name: "no logical name", current: &sink{}, logical: "", upstream: "127.0.0.1:1"},
		{name: "no upstream", current: &sink{}, logical: dependencyName, upstream: ""},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			proxy, err := opaque.Listen(
				t.Context(),
				"127.0.0.1:0",
				testCase.logical,
				testCase.upstream,
				testCase.current,
			)
			if err == nil {
				_ = proxy.Close()

				t.Fatal("Listen() accepted a configuration that cannot observe anything")
			}
		})
	}
}

// TestCloseIsIdempotent matters because teardown closes every proxy again after a failure.
func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	current := &sink{}
	proxy, done := startProxy(t, startUpstream(t, answerLines("+OK\n")), current)

	closeProxy(t, proxy, done)

	if err := proxy.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}
