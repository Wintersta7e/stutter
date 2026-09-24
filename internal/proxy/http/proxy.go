// Package http provides a cleartext HTTP/1.1 stub that records outbound requests.
package http

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	nethttp "net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Wintersta7e/stutter/internal/effect"
)

const (
	maxRequestBody        = 1 << 20
	maxRequestHeader      = 1 << 20
	initialHeaderCapacity = 1024
	tlsRecord             = 0x16
	// headerBound is how long a connection has to deliver its first request header: from accept in
	// cleartext, from handshake completion behind TLS. net/http's handshake timeout derives from it.
	headerBound = time.Second
)

var (
	errActiveRun       = errors.New("HTTP script already has an active run")
	errServerLog       = errors.New("the HTTP stub could not serve a connection")
	errEncrypted       = errors.New("client used TLS with the HTTP stub; cleartext HTTP/1.1 is required")
	errHandshake       = errors.New("TLS handshake with the HTTP stub failed")
	errInactiveRun     = errors.New("HTTP script run is not active")
	errMissingHost     = errors.New("HTTP proxy requires a stable logical host")
	errMissingScript   = errors.New("HTTP proxy requires a script")
	errMissingSink     = errors.New("HTTP proxy requires an effect sink")
	errOversizeRequest = errors.New("HTTP request body exceeds configured limit")
	errUnparseable     = errors.New("client traffic is not parseable HTTP/1.1; effects cannot be observed")
	errNoRequest       = errors.New("the client completed a TLS handshake and sent no request: " +
		"application-level certificate pinning or a protocol the stub does not serve")
)

// Sink receives the effects the stub observes and supplies the run canonical form used as a
// frozen-script key.
type Sink interface {
	Record(observation effect.Observation)
	Canonicalise(raw string) string
}

// Response is an immutable reply configuration. NewScript copies headers and body before use.
type Response struct {
	Header     nethttp.Header
	Body       []byte
	StatusCode int
}

// Route selects a response for one endpoint. Empty Method and Host match any method and host.
// Paths are exact; an empty Path matches only the root path.
type Route struct {
	Method   string
	Host     string
	Path     string
	Response Response
}

// Script is the persistent fixture shared by clean and faulted replay runs.
type Script struct {
	frozen          map[string][]Response
	active          *Run
	routes          []Route
	defaultResponse Response
	mu              sync.Mutex
}

// Run scopes one replay against a Script. The first committed run captures configured responses;
// later runs consume its frozen response queues.
type Run struct {
	script   *Script
	captured map[string][]Response
	replayed map[string]int
	capture  bool
	done     bool
}

// NewScript constructs a fixture. Its configuration is copied, so later caller mutation cannot
// change a run's replies.
func NewScript(defaultResponse Response, routes []Route) *Script {
	copyRoutes := make([]Route, len(routes))
	for index, route := range routes {
		copyRoutes[index] = Route{
			Method:   route.Method,
			Host:     route.Host,
			Path:     route.Path,
			Response: normaliseResponse(route.Response),
		}
	}

	return &Script{
		defaultResponse: normaliseResponse(defaultResponse),
		routes:          copyRoutes,
	}
}

// Begin activates a run. A call served outside an active run receives the default and is visibly
// marked off-script because it cannot be attributed to a captured replay.
func (s *Script) Begin() (*Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active != nil {
		return nil, errActiveRun
	}

	run := &Run{
		script:   s,
		captured: make(map[string][]Response),
		replayed: make(map[string]int),
		capture:  s.frozen == nil,
	}
	s.active = run

	return run, nil
}

// Commit finishes a run. The first run atomically becomes the immutable source for future runs.
func (r *Run) Commit() error {
	r.script.mu.Lock()
	defer r.script.mu.Unlock()

	if r.done || r.script.active != r {
		return errInactiveRun
	}

	if r.capture {
		r.script.frozen = cloneFrozen(r.captured)
	}

	r.done = true
	r.script.active = nil

	return nil
}

// Abort discards an unfinished capture and releases the fixture for a later run. It is safe to
// call during failure cleanup after Commit or another Abort.
func (r *Run) Abort() error {
	r.script.mu.Lock()
	defer r.script.mu.Unlock()

	if r.done {
		return nil
	}

	if r.script.active != r {
		return errInactiveRun
	}

	r.done = true
	r.script.active = nil

	return nil
}

// Proxy serves configured HTTP replies and records each request before its response is written.
type Proxy struct {
	listener net.Listener
	script   *Script
	sink     Sink
	server   *nethttp.Server
	closeErr error
	// failure is the first error net/http reported while serving. It stops the run rather than
	// being recorded, so a broken connection can never be mistaken for a side effect.
	failure error

	logicalHost string
	closeOnce   sync.Once
	failOnce    sync.Once
	failed      sync.Mutex
}

// Listen binds a cleartext HTTP/1.1 stub at addr. logicalHost replaces the ephemeral listener
// address in observations so runs can be compared.
func Listen(ctx context.Context, addr, logicalHost string, sink Sink, script *Script) (*Proxy, error) {
	return bind(ctx, addr, logicalHost, sink, script, nil)
}

// ListenTLS binds an HTTPS stub serving certificate.
//
// Most real dependencies are reached over TLS, and a service that cannot reach its dependency
// produces no effects at all — which reads as a handler that did nothing. The caller supplies the
// certificate and is responsible for giving the service under test the CA that signed it.
func ListenTLS(
	ctx context.Context,
	addr, logicalHost string,
	sink Sink,
	script *Script,
	certificate tls.Certificate,
) (*Proxy, error) {
	return bind(ctx, addr, logicalHost, sink, script, &tls.Config{
		Certificates: []tls.Certificate{certificate},
		// HTTP/1.1 is all the stub serves. Agreeing on it in the handshake makes an h2-only client
		// fail there, loudly, instead of connecting and hanging up unseen.
		NextProtos: []string{"http/1.1"},
		MinVersion: tls.VersionTLS12,
	})
}

func bind(
	ctx context.Context,
	addr, logicalHost string,
	sink Sink,
	script *Script,
	serverTLS *tls.Config,
) (*Proxy, error) {
	if sink == nil {
		return nil, errMissingSink
	}

	if script == nil {
		return nil, errMissingScript
	}

	if logicalHost == "" {
		return nil, errMissingHost
	}

	var config net.ListenConfig

	listener, err := config.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	proxy := &Proxy{logicalHost: logicalHost, script: script, sink: sink}

	// The cleartext guard runs in Accept and would reject every ClientHello. Behind TLS each
	// connection instead applies the same first-request rules to its decrypted stream, after its
	// handshake and on its own goroutine, and a failure there stops the run just as it does in
	// cleartext.
	proxy.listener = checkedListener{Listener: listener, readTimeout: headerBound}
	if serverTLS != nil {
		proxy.listener = tunnelListener{Listener: listener, config: serverTLS, proxy: proxy}
	}

	proxy.server = &nethttp.Server{
		Handler:           nethttp.HandlerFunc(proxy.serveRequest),
		ErrorLog:          log.New(proxyErrorWriter{proxy: proxy}, "", 0),
		ReadHeaderTimeout: headerBound,
	}

	return proxy, nil
}

// checkedListener rejects encrypted or malformed first requests before net/http can turn them into
// an unobservable 400 response. Returning the error from Accept stops Serve, which makes the whole
// run a setup error instead of a false clean result.
type checkedListener struct {
	net.Listener

	readTimeout time.Duration
}

func (l checkedListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, fmt.Errorf("accept HTTP connection: %w", err)
	}

	checked, err := checkConnection(connection, l.readTimeout, errUnparseable)
	if err != nil {
		_ = connection.Close()

		return nil, err
	}

	return checked, nil
}

// tunnelListener hands each TLS connection to net/http before its handshake, as tls.NewListener
// does, so no client's handshake or first request can delay another's accept.
type tunnelListener struct {
	net.Listener

	config *tls.Config
	proxy  *Proxy
}

func (l tunnelListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, fmt.Errorf("accept HTTPS connection: %w", err)
	}

	tunnel := &tunnel{proxy: l.proxy}
	config := l.config.Clone()
	config.GetConfigForClient = tunnel.hello
	tunnel.Conn = tls.Server(connection, config)

	return tunnel, nil
}

// tunnel is one TLS connection to the stub. net/http completes its handshake through
// HandshakeContext; its first Read then passes the decrypted stream through checkConnection.
type tunnel struct {
	*tls.Conn

	checked    net.Conn
	failure    error
	proxy      *Proxy
	serverName string
}

// HandshakeContext stops the run on a failed handshake, naming the server name the client asked
// for. The error goes back to net/http unchanged, so its answer to a cleartext client is too.
func (t *tunnel) HandshakeContext(ctx context.Context) error {
	err := t.Conn.HandshakeContext(ctx)
	if err != nil {
		t.proxy.fail(t.named(fmt.Errorf("%w: %w", errHandshake, err)))
	}

	return err //nolint:wrapcheck // net/http type-checks this error to answer a cleartext client.
}

// Read applies the cleartext first-request check to the decrypted stream before net/http sees any
// of it. net/http reads first right after the handshake, so the header bound runs from there.
func (t *tunnel) Read(buffer []byte) (int, error) {
	if t.checked == nil && t.failure == nil {
		t.checked, t.failure = checkConnection(t.Conn, headerBound, errNoRequest)
		if t.failure != nil {
			t.failure = t.named(t.failure)
			t.proxy.fail(t.failure)
		}
	}

	if t.failure != nil {
		return 0, t.failure
	}

	return t.checked.Read(buffer) //nolint:wrapcheck // Preserve io.Reader byte-count and error semantics.
}

// hello records the server name the client asked for. It is taken from the ClientHello because a
// handshake that fails on ALPN never stores it on the connection.
func (t *tunnel) hello(info *tls.ClientHelloInfo) (*tls.Config, error) {
	t.serverName = info.ServerName

	return nil, nil //nolint:nilnil // A nil config keeps the listener's own, as crypto/tls documents.
}

func (t *tunnel) named(err error) error {
	if t.serverName == "" {
		return fmt.Errorf("TLS connection without SNI: %w", err)
	}

	return fmt.Errorf("TLS connection for %q: %w", t.serverName, err)
}

type replayConn struct {
	net.Conn

	reader io.Reader
}

func (c *replayConn) Read(buffer []byte) (int, error) {
	return c.reader.Read(buffer) //nolint:wrapcheck // Preserve io.Reader byte-count and error semantics.
}

// checkConnection admits a connection only once its first request header parses as HTTP/1.x.
// noRequest is the error for one that closes, or stays silent past timeout, before its first byte.
func checkConnection(connection net.Conn, timeout time.Duration, noRequest error) (net.Conn, error) {
	if err := connection.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("bound HTTP header read: %w", err)
	}

	reader := bufio.NewReader(connection)

	first, err := reader.Peek(1)
	if err != nil {
		return nil, noRequest
	}

	if first[0] == tlsRecord {
		return nil, errEncrypted
	}

	header, err := readRequestHeader(reader)
	if err != nil {
		return nil, errUnparseable
	}

	request, err := nethttp.ReadRequest(bufio.NewReader(bytes.NewReader(header)))
	if err != nil || request.ProtoMajor != 1 {
		return nil, errUnparseable
	}

	if err := connection.SetReadDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("clear HTTP header deadline: %w", err)
	}

	return &replayConn{Conn: connection, reader: io.MultiReader(bytes.NewReader(header), reader)}, nil
}

func readRequestHeader(reader *bufio.Reader) ([]byte, error) {
	header := make([]byte, 0, initialHeaderCapacity)

	for len(header) <= maxRequestHeader {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, errUnparseable
		}

		header = append(header, line...)
		if bytes.Equal(line, []byte("\r\n")) || bytes.Equal(line, []byte("\n")) {
			return header, nil
		}
	}

	return nil, errUnparseable
}

// Addr is the address the proxy is listening on.
func (p *Proxy) Addr() string { return p.listener.Addr().String() }

// Serve accepts connections until Close is called or ctx is cancelled.
func (p *Proxy) Serve(ctx context.Context) error {
	stopped := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			_ = p.Close()
		case <-stopped:
		}
	}()

	err := p.server.Serve(p.listener)

	close(stopped)

	// A serving failure outranks the closure it caused: Close is how fail stops the run, so the
	// listener being shut would otherwise read as an orderly stop.
	if failure := p.serveFailure(); failure != nil {
		return failure
	}

	if errors.Is(err, nethttp.ErrServerClosed) || errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
		//nolint:nilerr // listener closure and context cancellation deliberately stop Serve.
		return nil
	}

	return fmt.Errorf("serve HTTP proxy: %w", err)
}

// Close immediately stops accepting requests and closes active HTTP connections.
func (p *Proxy) Close() error {
	p.closeOnce.Do(func() {
		p.closeErr = p.server.Close()
		if errors.Is(p.closeErr, nethttp.ErrServerClosed) || errors.Is(p.closeErr, net.ErrClosed) {
			p.closeErr = nil
		}
	})

	return p.closeErr
}

// CloseContext stops accepting requests and waits for active handlers to finish. The harness uses
// it only after the service has closed its HTTP connections, matching the forwarding proxies'
// blocking teardown contract.
func (p *Proxy) CloseContext(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.closeErr = p.server.Shutdown(ctx)
		if errors.Is(p.closeErr, nethttp.ErrServerClosed) || errors.Is(p.closeErr, net.ErrClosed) {
			p.closeErr = nil
		}
	})

	return p.closeErr
}

// fail records the first serving failure and stops the stub, so Serve reports it.
func (p *Proxy) fail(err error) {
	p.failOnce.Do(func() {
		p.failed.Lock()
		p.failure = err
		p.failed.Unlock()

		_ = p.Close()
	})
}

func (p *Proxy) serveFailure() error {
	p.failed.Lock()
	defer p.failed.Unlock()

	return p.failure
}

func (p *Proxy) serveRequest(writer nethttp.ResponseWriter, request *nethttp.Request) {
	body, readErr := readBody(request.Body)
	raw := renderRequest(request, p.logicalHost, p.Addr(), body, readErr)
	key := p.sink.Canonicalise(raw)
	response, observation := p.script.reply(key, request, p.logicalHost, p.Addr())
	observation.Kind = effect.KindHTTP
	observation.Raw = raw
	observation.Printable = raw
	p.sink.Record(observation)

	if readErr != nil {
		writer.Header().Set("Connection", "close")
		writer.WriteHeader(nethttp.StatusRequestEntityTooLarge)

		return
	}

	writeResponse(writer, response)
}

func (s *Script) reply(
	key string,
	request *nethttp.Request,
	logicalHost string,
	listenerAddr string,
) (Response, effect.Observation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active == nil {
		return s.defaultResponse, effect.Observation{Stubbed: true, OffScript: true}
	}

	if s.active.capture {
		response := s.routeResponse(request, logicalHost, listenerAddr)
		s.active.captured[key] = append(s.active.captured[key], cloneResponse(response))

		return response, effect.Observation{}
	}

	occurrence := s.active.replayed[key]
	queue := s.frozen[key]

	if occurrence >= len(queue) {
		return s.defaultResponse, effect.Observation{Stubbed: true, OffScript: true}
	}

	s.active.replayed[key]++

	return cloneResponse(queue[occurrence]), effect.Observation{Stubbed: true}
}

func (s *Script) routeResponse(request *nethttp.Request, logicalHost, listenerAddr string) Response {
	host := stableHost(request.Host, logicalHost, listenerAddr)
	for _, route := range s.routes {
		if route.Path != request.URL.Path {
			continue
		}

		if route.Method != "" && !strings.EqualFold(route.Method, request.Method) {
			continue
		}

		if route.Host != "" && !strings.EqualFold(route.Host, host) {
			continue
		}

		return cloneResponse(route.Response)
	}

	return cloneResponse(s.defaultResponse)
}

func readBody(body io.ReadCloser) ([]byte, error) {
	defer func() { _ = body.Close() }()

	limited, err := io.ReadAll(io.LimitReader(body, maxRequestBody+1))
	if err != nil {
		return limited, fmt.Errorf("read request body: %w", err)
	}

	if len(limited) > maxRequestBody {
		return limited[:maxRequestBody], errOversizeRequest
	}

	return limited, nil
}

// renderRequest assembles the comparable, readable form of one outbound request.
//
// It stays on ONE line, as the Postgres and NATS renderings do. The report indents each effect
// beneath its finding, so a newline inside an effect breaks the layout of every line after it.
func renderRequest(
	request *nethttp.Request,
	logicalHost, listenerAddr string,
	body []byte,
	readErr error,
) string {
	target := stableHost(request.Host, logicalHost, listenerAddr) + escapedPath(request.URL)
	if query := sortedQuery(request.URL.Query()); query != "" {
		target += "?" + query
	}

	fields := []string{strings.ToUpper(request.Method) + " " + target}

	if headers := strings.Join(sortedHeaders(request.Header), ", "); headers != "" {
		fields = append(fields, "headers=["+headers+"]")
	}

	if len(body) > 0 {
		fields = append(fields, "body="+renderBody(body, request.Header.Get("Content-Type")))
	}

	if readErr != nil {
		fields = append(fields, "request-error="+readErr.Error())
	}

	return strings.Join(fields, " ")
}

func stableHost(host, logicalHost, listenerAddr string) string {
	if host == "" || host == listenerAddr {
		return strings.ToLower(logicalHost)
	}

	return strings.ToLower(host)
}

func escapedPath(current *url.URL) string {
	path := current.EscapedPath()
	if path == "" {
		return "/"
	}

	return path
}

func sortedQuery(query url.Values) string {
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	var rendered []string

	for _, key := range keys {
		values := append([]string(nil), query[key]...)
		slices.Sort(values)

		for _, value := range values {
			rendered = append(rendered, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}

	return strings.Join(rendered, "&")
}

func sortedHeaders(header nethttp.Header) []string {
	keys := make([]string, 0, len(header))
	for key := range header {
		if ignoredHeader(key, header) {
			continue
		}

		keys = append(keys, strings.ToLower(key))
	}

	slices.Sort(keys)

	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		values := append([]string(nil), header.Values(key)...)
		slices.Sort(values)
		lines = append(lines, key+": "+strings.Join(values, ","))
	}

	return lines
}

func ignoredHeader(key string, header nethttp.Header) bool {
	switch strings.ToLower(key) {
	case "authorization", "content-length", "date", "traceparent", "user-agent", "x-request-id",
		"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
		"transfer-encoding", "upgrade":
		return true
	}

	for _, item := range header.Values("Connection") {
		for nominated := range strings.SplitSeq(item, ",") {
			if strings.EqualFold(strings.TrimSpace(nominated), key) {
				return true
			}
		}
	}

	return false
}

func renderBody(body []byte, contentType string) string {
	if strings.Contains(strings.ToLower(contentType), "json") {
		var decoded any
		if json.Unmarshal(body, &decoded) == nil {
			encoded, err := json.Marshal(decoded)
			if err == nil {
				return string(encoded)
			}
		}
	}

	if !utf8Text(body) {
		return "hex:" + hex.EncodeToString(body)
	}

	// Whitespace is collapsed for the same reason the NATS payload renderer collapses it: a body
	// that spans lines would otherwise split one effect across several report lines.
	return "text:" + strings.Join(strings.Fields(string(body)), " ")
}

func utf8Text(body []byte) bool {
	for len(body) > 0 {
		runeValue, width := utf8.DecodeRune(body)
		if runeValue == '\uFFFD' && width == 1 {
			return false
		}

		body = body[width:]
	}

	return true
}

func writeResponse(writer nethttp.ResponseWriter, response Response) {
	for key, values := range response.Header {
		for _, value := range values {
			writer.Header().Add(key, value)
		}
	}

	if !hasHeader(response.Header, "Date") {
		// A nil Date value suppresses net/http's wall-clock header. Letting the server invent one
		// would make a supposedly frozen reply differ on every replay.
		writer.Header()["Date"] = nil
	}

	writer.WriteHeader(response.StatusCode)

	if _, err := writer.Write(response.Body); err != nil {
		return
	}
}

func hasHeader(header nethttp.Header, name string) bool {
	for key := range header {
		if strings.EqualFold(key, name) {
			return true
		}
	}

	return false
}

func normaliseResponse(response Response) Response {
	if response.StatusCode == 0 && len(response.Header) == 0 && len(response.Body) == 0 {
		response = Response{
			Header:     nethttp.Header{"Content-Type": {"application/json"}},
			Body:       []byte("{}"),
			StatusCode: nethttp.StatusOK,
		}
	} else if response.StatusCode == 0 {
		response.StatusCode = nethttp.StatusOK
	}

	return cloneResponse(response)
}

func cloneResponse(response Response) Response {
	return Response{
		Header:     response.Header.Clone(),
		Body:       append([]byte(nil), response.Body...),
		StatusCode: response.StatusCode,
	}
}

func cloneFrozen(source map[string][]Response) map[string][]Response {
	copyMap := make(map[string][]Response, len(source))
	for key, queue := range source {
		copyQueue := make([]Response, len(queue))
		for index, response := range queue {
			copyQueue[index] = cloneResponse(response)
		}

		copyMap[key] = copyQueue
	}

	return copyMap
}

// proxyErrorWriter turns net/http's error log into a run-stopping failure.
//
// A failed TLS handshake or an unparseable request must NEVER become an effect. Recorded as one it
// appears twice under duplicate delivery and is reported as a divergence that is entirely an
// artefact of the broken connection — a false positive manufactured out of a connection error.
// Certificate pinning and embedded certificate pools are the usual cause, and they have to fail
// loudly rather than quietly turn into findings.
type proxyErrorWriter struct{ proxy *Proxy }

func (w proxyErrorWriter) Write(message []byte) (int, error) {
	w.proxy.fail(fmt.Errorf("%w: %s", errServerLog, strings.TrimSpace(string(message))))

	return len(message), nil
}
