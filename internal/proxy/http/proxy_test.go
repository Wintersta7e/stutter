package http_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/effect"
	proxyhttp "github.com/Wintersta7e/stutter/internal/proxy/http"
)

type sink struct {
	observations []effect.Observation
	mu           sync.Mutex
}

func (s *sink) Record(observation effect.Observation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.observations = append(s.observations, observation)
}

func (*sink) Canonicalise(raw string) string {
	canonical := regexp.MustCompile(`(?i)[0-9a-f]{8}-[0-9a-f-]{27}`).ReplaceAllString(raw, "<uuid>")

	return regexp.MustCompile(`20[0-9]{2}-[0-9T:.+Z-]+`).ReplaceAllString(canonical, "<ts>")
}

func (s *sink) all() []effect.Observation {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]effect.Observation(nil), s.observations...)
}

func TestLifecycleAndCleanShutdown(t *testing.T) {
	t.Parallel()

	current := &sink{}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	proxy, done := startProxy(t, current, script)

	run, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	response := doRequest(t, proxy, http.MethodGet, "/", "")
	if response.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.status)
	}

	if got := response.header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}

	if got := response.header.Get("Date"); got != "" {
		t.Errorf("Date = %q, want the wall-clock header suppressed", got)
	}

	if commitErr := run.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	closeProxy(t, proxy, done)
}

func TestAbortDiscardsAPartialCapture(t *testing.T) {
	t.Parallel()

	current := &sink{}
	script := proxyhttp.NewScript(
		proxyhttp.Response{Body: []byte("default")},
		[]proxyhttp.Route{
			{Path: "/aborted", Response: proxyhttp.Response{Body: []byte("must not freeze")}},
			{Path: "/kept", Response: proxyhttp.Response{Body: []byte("frozen")}},
		},
	)
	proxy, done := startProxy(t, current, script)

	aborted, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_ = doRequest(t, proxy, http.MethodGet, "/aborted", "")

	if abortErr := aborted.Abort(); abortErr != nil {
		t.Fatal(abortErr)
	}

	if abortErr := aborted.Abort(); abortErr != nil {
		t.Fatalf("second Abort() error = %v, want idempotent cleanup", abortErr)
	}

	capture, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_ = doRequest(t, proxy, http.MethodGet, "/kept", "")
	if commitErr := capture.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	replay, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	miss := doRequest(t, proxy, http.MethodGet, "/aborted", "")

	kept := doRequest(t, proxy, http.MethodGet, "/kept", "")
	if commitErr := replay.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	closeProxy(t, proxy, done)

	if got := string(miss.body); got != "default" {
		t.Errorf("aborted response = %q, want default", got)
	}

	if got := string(kept.body); got != "frozen" {
		t.Errorf("committed response = %q, want frozen", got)
	}

	observations := current.all()
	if !observations[len(observations)-2].OffScript {
		t.Error("request from the aborted capture was replayed as on-script")
	}
}

func TestRequestRenderingIsStableAndRedactsAuthorization(t *testing.T) {
	t.Parallel()

	current := &sink{}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	proxy, done := startProxy(t, current, script)

	run, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"http://"+proxy.Addr()+"/a%20b?z=2&a=second&a=first",
		strings.NewReader(`{"z":1,"a":{"y":2,"x":1}}`),
	)
	if err != nil {
		t.Fatal(err)
	}

	request.Header.Set("Authorization", "Bearer secret")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Zebra", "last")
	request.Header.Set("X-Alpha", "first")
	request.Header.Set("User-Agent", "unstable")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}

	_ = response.Body.Close()

	if commitErr := run.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	closeProxy(t, proxy, done)

	observations := current.all()
	if len(observations) != 1 {
		t.Fatalf("observations = %d, want 1", len(observations))
	}

	// One effect stays on one line, as it does for Postgres and NATS: the report indents effects
	// beneath a finding, and a newline here would break every line after it.
	want := `POST logical.test/a%20b?a=first&a=second&z=2 ` +
		`headers=[accept-encoding: gzip, content-type: application/json, ` +
		`x-alpha: first, x-zebra: last] body={"a":{"x":1,"y":2},"z":1}`
	if observations[0].Raw != want {
		t.Errorf("raw\n got: %s\nwant: %s", observations[0].Raw, want)
	}

	if strings.Contains(observations[0].Raw+observations[0].Printable, "secret") {
		t.Error("authorization leaked into observation")
	}
}

func TestMultilineBodyStaysOneEffectLine(t *testing.T) {
	t.Parallel()

	current := &sink{}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	proxy, done := startProxy(t, current, script)

	run, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"http://"+proxy.Addr()+"/notes",
		strings.NewReader("first line\r\nsecond\tline\n"),
	)
	if err != nil {
		t.Fatal(err)
	}

	request.Header.Set("Content-Type", "text/plain")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}

	_ = response.Body.Close()

	if commitErr := run.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	closeProxy(t, proxy, done)

	observations := current.all()
	if len(observations) != 1 {
		t.Fatalf("observations = %d, want 1", len(observations))
	}

	if strings.ContainsAny(observations[0].Raw, "\r\n") {
		t.Errorf("effect spans more than one line: %q", observations[0].Raw)
	}

	if !strings.Contains(observations[0].Raw, "body=text:first line second line") {
		t.Errorf("body was not collapsed onto one line: %q", observations[0].Raw)
	}
}

func TestCaptureAndReplayUseFrozenResponses(t *testing.T) {
	t.Parallel()

	current := &sink{}
	configured := proxyhttp.Response{
		Header:     http.Header{"X-Reply": {"configured"}},
		Body:       []byte("first-body"),
		StatusCode: http.StatusCreated,
	}
	script := proxyhttp.NewScript(
		proxyhttp.Response{Body: []byte("default"), StatusCode: http.StatusOK},
		[]proxyhttp.Route{{
			Method: http.MethodPost, Host: "logical.test", Path: "/items", Response: configured,
		}},
	)
	proxy, done := startProxy(t, current, script)

	first, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	firstReply := doRequest(t, proxy, http.MethodPost, "/items", `{"id":1}`)
	if commitErr := first.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	configured.Body[0] = 'X'
	configured.Header.Set("X-Reply", "mutated")

	second, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	secondReply := doRequest(t, proxy, http.MethodPost, "/items", `{"id":1}`)
	if commitErr := second.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	closeProxy(t, proxy, done)

	if firstReply.status != http.StatusCreated || secondReply.status != firstReply.status ||
		!bytes.Equal(firstReply.body, secondReply.body) || !reflect.DeepEqual(firstReply.header, secondReply.header) ||
		secondReply.header.Get("X-Reply") != "configured" || secondReply.header.Get("Date") != "" {
		t.Fatalf("replies were not byte-identical: first=%+v second=%+v", firstReply, secondReply)
	}

	observations := current.all()
	if observations[0].Stubbed || observations[0].OffScript || !observations[1].Stubbed || observations[1].OffScript {
		t.Fatalf("flags = %+v, %+v", observations[0], observations[1])
	}
}

func TestFrozenMatchUsesRecorderCanonicalisation(t *testing.T) {
	t.Parallel()

	const (
		messageID = "ORDER-9001"
		firstID   = "123e4567-e89b-12d3-a456-426614174000"
		secondID  = "123e4567-e89b-12d3-a456-426614174999"
	)

	script := proxyhttp.NewScript(proxyhttp.Response{}, []proxyhttp.Route{{
		Path: "/event", Response: proxyhttp.Response{Body: []byte("frozen")},
	}})
	key := []byte("shared test hash key")
	payload := []byte(`{"order_id":"` + messageID + `"}`)

	firstRecorder := effect.NewRecorder(effect.NewCanonicaliser(), key)
	firstRecorder.Open("orders", 1, payload)

	capture, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	firstProxy, firstDone := startProxy(t, firstRecorder, script)

	_ = doRequest(
		t,
		firstProxy,
		http.MethodPost,
		"/event",
		`{"order_id":"`+messageID+`","request_id":"`+firstID+`","at":"2026-09-04T10:00:00Z"}`,
	)
	if commitErr := capture.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	closeProxy(t, firstProxy, firstDone)

	secondRecorder := effect.NewRecorder(effect.NewCanonicaliser(), key)
	secondRecorder.Open("orders", 1, payload)

	replay, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	secondProxy, secondDone := startProxy(t, secondRecorder, script)

	_ = doRequest(
		t,
		secondProxy,
		http.MethodPost,
		"/event",
		`{"order_id":"`+messageID+`","request_id":"`+secondID+`","at":"2027-01-01T12:34:56Z"}`,
	)
	if commitErr := replay.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	closeProxy(t, secondProxy, secondDone)

	firstEffects := firstRecorder.Effects()

	secondEffects := secondRecorder.Effects()
	if len(firstEffects) != 1 || len(secondEffects) != 1 {
		t.Fatalf("effect counts = %d, %d, want one per run", len(firstEffects), len(secondEffects))
	}

	if firstEffects[0].Canonical != secondEffects[0].Canonical {
		t.Errorf(
			"canonical effects differ:\nfirst:  %s\nsecond: %s",
			firstEffects[0].Canonical,
			secondEffects[0].Canonical,
		)
	}

	if !secondEffects[0].Stubbed || secondEffects[0].OffScript {
		t.Errorf("replay metadata = stubbed %v, off-script %v", secondEffects[0].Stubbed, secondEffects[0].OffScript)
	}
}

func TestFrozenMatchCanonicalisesGeneratedValuesAndQueues(t *testing.T) {
	t.Parallel()

	current := &sink{}
	script := proxyhttp.NewScript(proxyhttp.Response{}, []proxyhttp.Route{{
		Path: "/event", Response: proxyhttp.Response{Body: []byte("queued")},
	}})
	proxy, done := startProxy(t, current, script)

	capture, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	first := doRequest(
		t,
		proxy,
		http.MethodPost,
		"/event",
		`{"id":"123e4567-e89b-12d3-a456-426614174000","at":"2026-09-04T10:00:00Z"}`,
	)

	second := doRequest(
		t,
		proxy,
		http.MethodPost,
		"/event",
		`{"id":"123e4567-e89b-12d3-a456-426614174000","at":"2026-09-04T10:00:00Z"}`,
	)
	if commitErr := capture.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	replay, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	firstAgain := doRequest(
		t,
		proxy,
		http.MethodPost,
		"/event",
		`{"id":"123e4567-e89b-12d3-a456-426614174999","at":"2027-01-01T12:34:56Z"}`,
	)
	secondAgain := doRequest(
		t,
		proxy,
		http.MethodPost,
		"/event",
		`{"id":"123e4567-e89b-12d3-a456-426614174999","at":"2027-01-01T12:34:56Z"}`,
	)

	thirdAgain := doRequest(
		t,
		proxy,
		http.MethodPost,
		"/event",
		`{"id":"123e4567-e89b-12d3-a456-426614174999","at":"2027-01-01T12:34:56Z"}`,
	)
	if commitErr := replay.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	closeProxy(t, proxy, done)

	if !bytes.Equal(first.body, firstAgain.body) || !bytes.Equal(second.body, secondAgain.body) {
		t.Fatalf("queue replay mismatch: %q %q %q %q", first.body, second.body, firstAgain.body, secondAgain.body)
	}

	if string(thirdAgain.body) != "{}" {
		t.Fatalf("queue exhaustion body = %q, want default", thirdAgain.body)
	}

	observations := current.all()
	if !observations[len(observations)-1].OffScript {
		t.Fatal("queue exhaustion was not marked off-script")
	}
}

func TestReplayMissUsesDefaultAndIsFlagged(t *testing.T) {
	t.Parallel()

	current := &sink{}
	script := proxyhttp.NewScript(proxyhttp.Response{Body: []byte("default")}, []proxyhttp.Route{{
		Path: "/known", Response: proxyhttp.Response{Body: []byte("route")},
	}})
	proxy, done := startProxy(t, current, script)

	capture, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_ = doRequest(t, proxy, http.MethodGet, "/known", "")
	if commitErr := capture.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	replay, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	miss := doRequest(t, proxy, http.MethodGet, "/other", "")
	if commitErr := replay.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	closeProxy(t, proxy, done)

	if got := string(miss.body); got != "default" {
		t.Fatalf("off-script body = %q, want default", got)
	}

	observations := current.all()
	if !observations[len(observations)-1].Stubbed || !observations[len(observations)-1].OffScript {
		t.Fatalf("miss flags = %+v", observations[len(observations)-1])
	}
}

func TestOversizeRequestIsVisibleAndDeterministic(t *testing.T) {
	t.Parallel()

	current := &sink{}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	proxy, done := startProxy(t, current, script)

	capture, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	body := strings.Repeat("x", (1<<20)+1)

	first := doRequest(t, proxy, http.MethodPost, "/large", body)
	if commitErr := capture.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	replay, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	second := doRequest(t, proxy, http.MethodPost, "/large", body)
	if commitErr := replay.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	closeProxy(t, proxy, done)

	if first.status != http.StatusRequestEntityTooLarge || second.status != first.status {
		t.Errorf("statuses = %d, %d, want two deterministic 413 replies", first.status, second.status)
	}

	observations := current.all()
	if len(observations) != 2 {
		t.Fatalf("observations = %d, want 2", len(observations))
	}

	for _, observation := range observations {
		if !strings.Contains(observation.Raw, "request-error=HTTP request body exceeds configured limit") {
			t.Errorf("oversize request was not surfaced in observation: %q", observation.Raw)
		}
	}

	if !observations[1].Stubbed || observations[1].OffScript {
		t.Errorf("replay metadata = stubbed %v, off-script %v", observations[1].Stubbed, observations[1].OffScript)
	}
}

func TestUnparsedHTTPFailsTheProxy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		want    string
		payload []byte
	}{
		{name: "TLS", payload: []byte{0x16, 0x03, 0x01, 0x00, 0x00}, want: "used TLS"},
		{name: "malformed request", payload: []byte("NOT HTTP\r\n\r\n"), want: "not parseable HTTP/1.1"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			current := &sink{}
			script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
			proxy, done := startProxy(t, current, script)

			run, err := script.Begin()
			if err != nil {
				t.Fatal(err)
			}

			dialer := net.Dialer{Timeout: time.Second}

			connection, err := dialer.DialContext(t.Context(), "tcp", proxy.Addr())
			if err != nil {
				t.Fatal(err)
			}

			if _, err := connection.Write(testCase.payload); err != nil {
				t.Fatal(err)
			}

			_ = connection.Close()

			select {
			case serveErr := <-done:
				if serveErr == nil || !strings.Contains(serveErr.Error(), testCase.want) {
					t.Fatalf("Serve() error = %v, want it to contain %q", serveErr, testCase.want)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Serve did not fail after unparseable traffic")
			}

			if abortErr := run.Abort(); abortErr != nil {
				t.Fatal(abortErr)
			}

			if closeErr := proxy.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}

			if got := len(current.all()); got != 0 {
				t.Errorf("recorded %d comparable effects for traffic that should stop the run", got)
			}
		})
	}
}

func TestPersistentConnection(t *testing.T) {
	t.Parallel()

	current := &sink{}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	proxy, done := startProxy(t, current, script)

	run, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: false}}
	defer client.CloseIdleConnections()

	for range 2 {
		request, requestErr := http.NewRequestWithContext(
			t.Context(),
			http.MethodGet,
			"http://"+proxy.Addr()+"/same",
			nil,
		)
		if requestErr != nil {
			t.Fatal(requestErr)
		}

		response, requestErr := client.Do(request)
		if requestErr != nil {
			t.Fatal(requestErr)
		}

		if _, copyErr := io.Copy(io.Discard, response.Body); copyErr != nil {
			t.Fatal(copyErr)
		}

		_ = response.Body.Close()
	}

	if commitErr := run.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	closeProxy(t, proxy, done)

	if got := len(current.all()); got != 2 {
		t.Fatalf("observations = %d, want 2", got)
	}
}

func TestCloseContextStopsAfterIdleConnectionsClose(t *testing.T) {
	t.Parallel()

	current := &sink{}
	script := proxyhttp.NewScript(proxyhttp.Response{}, nil)
	proxy, done := startProxy(t, current, script)

	run, err := script.Begin()
	if err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Transport: &http.Transport{}}

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+proxy.Addr()+"/", nil)
	if err != nil {
		t.Fatal(err)
	}

	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}

	_ = response.Body.Close()

	client.CloseIdleConnections()

	if commitErr := run.Commit(); commitErr != nil {
		t.Fatal(commitErr)
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	if err := proxy.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}

	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type reply struct {
	header http.Header
	body   []byte
	status int
}

func doRequest(t *testing.T, proxy *proxyhttp.Proxy, method, path, body string) reply {
	t.Helper()

	request, err := http.NewRequestWithContext(
		t.Context(),
		method,
		"http://"+proxy.Addr()+path,
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}

	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = response.Body.Close() }()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}

	return reply{body: responseBody, header: response.Header.Clone(), status: response.StatusCode}
}

func startProxy(t *testing.T, current proxyhttp.Sink, script *proxyhttp.Script) (*proxyhttp.Proxy, <-chan error) {
	t.Helper()

	proxy, err := proxyhttp.Listen(t.Context(), "127.0.0.1:0", "logical.test", current, script)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- proxy.Serve(t.Context()) }()

	return proxy, done
}

func closeProxy(t *testing.T, proxy *proxyhttp.Proxy, done <-chan error) {
	t.Helper()

	if err := proxy.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after Close")
	}
}
