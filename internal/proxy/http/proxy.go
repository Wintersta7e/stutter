// Package http provides an HTTP/1.1 stub, in cleartext and over TLS, that records outbound requests.
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
	"net/netip"
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
	// cleartextPort and tlsPort are the ports the service dials the stub's two entries on. A stop names
	// them, never the listener's own kernel-assigned port.
	cleartextPort uint16 = 80
	tlsPort       uint16 = 443
	// headerBound is how long a connection has to deliver its first request header, from its first
	// byte. net/http's handshake timeout derives from it.
	headerBound = time.Second
	// http2Preface opens every HTTP/2 connection made with prior knowledge.
	http2Preface = "PRI * HTTP/2.0\r\n"
)

var (
	errActiveRun       = errors.New("HTTP script already has an active run")
	errServerLog       = errors.New("the HTTP stub could not serve a connection")
	errEncrypted       = errors.New("client used TLS with the HTTP stub; cleartext HTTP/1.1 is required")
	errInactiveRun     = errors.New("HTTP script run is not active")
	errMissingHost     = errors.New("HTTP proxy requires a stable logical host")
	errMissingScript   = errors.New("HTTP proxy requires a script")
	errMissingSink     = errors.New("HTTP proxy requires an effect sink")
	errOversizeRequest = errors.New("HTTP request body exceeds configured limit")
	errUnparseable     = errors.New("client traffic is not parseable HTTP/1.1; effects cannot be observed")
	errPreface         = errors.New("client sent the HTTP/2 connection preface")
	errNoRequest       = errors.New(detailNoRequest)
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
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Proxy struct {
	script *Script
	sink   Sink
	server *nethttp.Server
	// tlsConfig is the one TLS configuration every connection shares: a copy per connection would give
	// each its own session ticket keys, and no client could resume a session.
	tlsConfig *tls.Config
	// held are the connections the stub is judging or holding before net/http has them, closed with
	// the stub so an idle one never presents as a teardown that hangs.
	held     map[net.Conn]struct{}
	closeErr error
	// failure is the first error net/http reported while serving. It stops the run rather than
	// being recorded, so a broken connection can never be mistaken for a side effect.
	failure error
	// served holds the server names at least one TLS connection has sent a request for. A later
	// connection to one of them that sends nothing is the client's pool, not a client that cannot
	// reach the stub (see tunnel.Read).
	served      sync.Map
	logicalHost string
	entries     []*entry

	closeOnce sync.Once
	failOnce  sync.Once
	failed    sync.Mutex
	heldMu    sync.Mutex
	// closing is set once the stub has begun to close, so its own closes are never read as a client's.
	closing bool
}

// New builds a stub serving entries. logicalHost replaces the addresses a service was told to dial in
// observations, so runs can be compared. certificate presents the TLS entry's certificate for each
// client; it is required when that entry is set.
//
// Most real dependencies are reached over TLS, and a service that cannot reach its dependency produces
// no effects at all — which reads as a handler that did nothing. The caller is responsible for giving
// the service under test the CA that signed what certificate returns.
func New(
	entries Entries,
	logicalHost string,
	sink Sink,
	script *Script,
	certificate func(*tls.ClientHelloInfo) (*tls.Certificate, error),
) (*Proxy, error) {
	switch {
	case sink == nil:
		return nil, errMissingSink
	case script == nil:
		return nil, errMissingScript
	case logicalHost == "":
		return nil, errMissingHost
	case entries.Cleartext == nil && entries.TLS == nil:
		return nil, errNoEntries
	case entries.TLS != nil && certificate == nil:
		return nil, errNoCertificates
	default:
	}

	proxy := &Proxy{
		logicalHost: logicalHost,
		script:      script,
		sink:        sink,
		held:        make(map[net.Conn]struct{}),
		tlsConfig: &tls.Config{
			GetCertificate:     certificate,
			GetConfigForClient: recordServerName,
			// HTTP/1.1 is all the stub serves. Agreeing on it in the handshake makes an h2-only client
			// fail there, loudly, instead of connecting and hanging up unseen.
			NextProtos: []string{"http/1.1"},
			MinVersion: tls.VersionTLS12,
		},
	}

	if entries.Cleartext != nil {
		proxy.entries = append(proxy.entries, newEntry(entries.Cleartext, proxy, entryCleartext, cleartextPort))
	}

	if entries.TLS != nil {
		proxy.entries = append(proxy.entries, newEntry(entries.TLS, proxy, entryTLS, tlsPort))
	}

	proxy.server = &nethttp.Server{
		Handler:           nethttp.HandlerFunc(proxy.serveRequest),
		ErrorLog:          log.New(proxyErrorWriter{proxy: proxy}, "", 0),
		ReadHeaderTimeout: headerBound,
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			return context.WithValue(ctx, connKey{}, conn)
		},
	}

	return proxy, nil
}

// connKey carries the connection a request arrived on, so a stop can name the port it was dialled on.
type connKey struct{}

// destination is where a connection handed to net/http was dialled: the entry's port, and the server
// name a TLS client asked for.
type destination interface {
	destination() (uint16, string)
}

// tunnel is one TLS connection to the stub. net/http completes its handshake through
// HandshakeContext; its first Read then passes the decrypted stream through checkConnection.
type tunnel struct {
	*tls.Conn

	checked    net.Conn
	failure    error
	proxy      *Proxy
	serverName string
	// offered are the application protocols the client's hello listed, sorted.
	offered []string
	port    uint16
}

// HandshakeContext stops the run on a failed handshake, naming the server name the client asked
// for. The error goes back to net/http unchanged, so its answer to a cleartext client is too.
func (t *tunnel) HandshakeContext(ctx context.Context) error {
	err := t.Conn.HandshakeContext(context.WithValue(ctx, tunnelKey{}, t))
	if err != nil {
		// A stop's text is compared between runs, so it says why in fixed words and never carries the
		// connection's addresses — an ephemeral port on every run.
		stop := &EgressStop{
			Name:   t.serverName,
			Detail: handshakeDetail(t.serverName, err),
			Class:  StopHandshake,
			Port:   t.port,
		}

		if refusesHTTP1(t.offered) {
			stop.Class, stop.Detail = StopALPN, alpnDetail(t.offered)
		}

		t.proxy.stopOn(stop)
	}

	return err //nolint:wrapcheck // net/http type-checks this error to answer a cleartext client.
}

// Read applies the cleartext first-request check to the decrypted stream before net/http sees any
// of it. net/http reads first right after the handshake, so the header bound runs from there.
func (t *tunnel) Read(buffer []byte) (int, error) {
	if t.checked == nil && t.failure == nil {
		t.checked, t.failure = checkConnection(t.Conn, headerBound, errNoRequest, t.port)

		switch {
		case t.failure == nil:
			t.proxy.served.Store(t.serverName, struct{}{})
		case errors.Is(t.failure, errNoRequest) && t.proxy.wasServed(t.serverName):
			// Go's http.Transport dials for a waiting request, hands that request a connection that
			// freed first, and pools the fresh one unused. Once this server name has been served, a
			// connection that sends nothing is that pool, so it closes quietly. A client that pins
			// certificates never gets a first connection through, so it still stops the run.
			t.failure = io.EOF
		default:
			t.failure = asStop(t.failure, t.port, t.serverName)
			t.proxy.stopOn(t.failure)
		}
	}

	if t.failure != nil {
		return 0, t.failure
	}

	return t.checked.Read(buffer) //nolint:wrapcheck // Preserve io.Reader byte-count and error semantics.
}

func (t *tunnel) destination() (uint16, string) { return t.port, t.serverName }

// tunnelKey carries a tunnel through its own handshake, so the one TLS config every connection
// shares can tell which tunnel a ClientHello belongs to. A config per connection would give each its
// own session ticket keys, and no client could resume a session.
type tunnelKey struct{}

// recordServerName hands each tunnel the server name its client asked for, and the protocols it
// offered. It runs on the ClientHello, before ALPN can fail the handshake, which is the only point
// every stop can name it.
func recordServerName(hello *tls.ClientHelloInfo) (*tls.Config, error) {
	if current, ok := hello.Context().Value(tunnelKey{}).(*tunnel); ok {
		current.serverName = hello.ServerName
		current.offered = slices.Sorted(slices.Values(hello.SupportedProtos))
	}

	return nil, nil //nolint:nilnil // A nil config keeps the listener's own, as crypto/tls documents.
}

type replayConn struct {
	net.Conn

	reader io.Reader
	port   uint16
}

func (c *replayConn) Read(buffer []byte) (int, error) {
	return c.reader.Read(buffer) //nolint:wrapcheck // Preserve io.Reader byte-count and error semantics.
}

func (c *replayConn) destination() (uint16, string) { return c.port, "" }

// checkConnection admits a connection only once its first request header parses as HTTP/1.x.
// noRequest is the error for one that closes, or stays silent past timeout, before its first byte.
func checkConnection(connection net.Conn, timeout time.Duration, noRequest error, port uint16) (net.Conn, error) {
	if err := connection.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("bound HTTP header read: %w", err)
	}

	first, err := firstByte(connection)
	if err != nil {
		return nil, noRequest
	}

	if first[0] == tlsRecord {
		return nil, errEncrypted
	}

	return checkRequest(connection, first, port)
}

// asStop turns checkConnection's verdict on a connection to the entry at port into the stop it is.
// name is the server name a TLS client asked for, empty in cleartext.
func asStop(err error, port uint16, name string) error {
	stop := &EgressStop{Name: name, Port: port}

	if connect, isConnect := errors.AsType[*connectError](err); isConnect {
		stop.Class, stop.Name = StopConnect, connect.target

		return stop
	}

	switch {
	case errors.Is(err, errEncrypted) && port == cleartextPort:
		stop.Class = StopTLSOnCleartext
	case errors.Is(err, errNoRequest):
		stop.Class = StopNoRequest
	case errors.Is(err, errPreface):
		stop.Class, stop.Detail = StopNotHTTP, detailPreface
	case errors.Is(err, errEncrypted), errors.Is(err, errUnparseable):
		stop.Class = StopNotHTTP
	default:
		return err
	}

	return stop
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

// Addr is the cleartext entry's address, else the TLS entry's.
func (p *Proxy) Addr() string { return p.entries[0].Addr().String() }

// Serve accepts connections on every entry until Close is called or ctx is cancelled, and returns the
// first failure.
func (p *Proxy) Serve(ctx context.Context) error {
	stopped := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done():
			_ = p.Close()
		case <-stopped:
		}
	}()

	results := make(chan error, len(p.entries))

	for _, current := range p.entries {
		go current.listen()
		go func() { results <- p.server.Serve(current) }()
	}

	var err error

	for range p.entries {
		served := <-results
		if !errors.Is(served, nethttp.ErrServerClosed) && !errors.Is(served, net.ErrClosed) && err == nil {
			// One entry failing ends the stub: the others serve the same run.
			err = served
			_ = p.Close()
		}
	}

	close(stopped)

	// A serving failure outranks the closure it caused: Close is how fail stops the run, so the
	// listener being shut would otherwise read as an orderly stop.
	if failure := p.serveFailure(); failure != nil {
		return failure
	}

	if err == nil || ctx.Err() != nil {
		//nolint:nilerr // listener closure and context cancellation deliberately stop Serve.
		return nil
	}

	return fmt.Errorf("serve HTTP proxy: %w", err)
}

// Close immediately stops accepting requests and closes active HTTP connections and held ones.
func (p *Proxy) Close() error {
	p.closeOnce.Do(func() {
		p.closeHeld()
		p.closeErr = p.server.Close()
		p.closeEntries()

		if errors.Is(p.closeErr, nethttp.ErrServerClosed) || errors.Is(p.closeErr, net.ErrClosed) {
			p.closeErr = nil
		}
	})

	return p.closeErr
}

// CloseContext stops accepting requests, closes held connections and waits for active handlers to
// finish. The harness uses it only after the service has closed its HTTP connections, matching the
// forwarding proxies' blocking teardown contract.
//
// A held connection is closed first: net/http counts a connection that has sent nothing as idle only
// after 5 s, so waiting for it would present an idle client as a teardown that hangs.
func (p *Proxy) CloseContext(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.closeHeld()
		p.closeErr = p.server.Shutdown(ctx)
		p.closeEntries()

		if errors.Is(p.closeErr, nethttp.ErrServerClosed) || errors.Is(p.closeErr, net.ErrClosed) {
			p.closeErr = nil
		}
	})

	return p.closeErr
}

// closeEntries closes every entry, whether or not Serve ever tracked it.
func (p *Proxy) closeEntries() {
	for _, current := range p.entries {
		_ = current.Close()
	}
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

// wasServed reports whether a TLS connection has already sent a request for serverName.
func (p *Proxy) wasServed(serverName string) bool {
	_, served := p.served.Load(serverName)

	return served
}

func (p *Proxy) serveFailure() error {
	p.failed.Lock()
	defer p.failed.Unlock()

	return p.failure
}

func (p *Proxy) serveRequest(writer nethttp.ResponseWriter, request *nethttp.Request) {
	// Checked before anything is recorded: the tunnel hides the request inside it.
	if request.Method == nethttp.MethodConnect {
		p.refuseConnect(writer, request)

		return
	}

	body, readErr := readBody(request.Body)
	raw := renderRequest(request, p.logicalHost, body, readErr)
	key := p.sink.Canonicalise(raw)
	response, observation := p.script.reply(key, request, p.logicalHost)
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

// refuseConnect stops the run on a CONNECT a connection sent after its first request, and hangs up
// on it rather than answering: an answer would tell the client its tunnel is open.
func (p *Proxy) refuseConnect(writer nethttp.ResponseWriter, request *nethttp.Request) {
	var port uint16

	if conn, isKnown := request.Context().Value(connKey{}).(destination); isKnown {
		port, _ = conn.destination()
	}

	p.fail(&EgressStop{Class: StopConnect, Name: request.Host, Port: port})

	if hijacker, canHijack := writer.(nethttp.Hijacker); canHijack {
		if conn, _, err := hijacker.Hijack(); err == nil {
			_ = conn.Close()

			return
		}
	}

	writer.Header().Set("Connection", "close")
	writer.WriteHeader(nethttp.StatusBadGateway)
}

func (s *Script) reply(key string, request *nethttp.Request, logicalHost string) (Response, effect.Observation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active == nil {
		return s.defaultResponse, effect.Observation{Stubbed: true, OffScript: true}
	}

	if s.active.capture {
		response := s.routeResponse(request, logicalHost)
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

func (s *Script) routeResponse(request *nethttp.Request, logicalHost string) Response {
	host := stableHost(request.Host, logicalHost)
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
func renderRequest(request *nethttp.Request, logicalHost string, body []byte, readErr error) string {
	target := stableHost(request.Host, logicalHost) + escapedPath(request.URL)
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

// stableHost is the host an effect names: the request's Host, lower-cased, its port kept. A Host that
// is empty, an IP literal or localhost is where the service was told the stub is — a bind and
// advertise choice, with a kernel-assigned port — so it renders as the logical host instead, and no
// run's addresses reach an effect.
func stableHost(host, logicalHost string) string {
	name := host
	if split, _, err := net.SplitHostPort(host); err == nil {
		name = split
	}

	name = strings.Trim(name, "[]")

	if _, err := netip.ParseAddr(name); name == "" || err == nil || strings.EqualFold(name, "localhost") {
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
