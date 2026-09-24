package dockertest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/testgate"
)

const (
	// maxSocketPath is the longest path a unix socket address holds, its terminating NUL aside.
	maxSocketPath = 107
	// headerTimeout bounds how long the recorder waits for a request's headers.
	headerTimeout = 30 * time.Second
	// maxBody bounds how much of a create's request or response body the recorder reads.
	maxBody = 16 << 20
	// maxStreamLine bounds one line of a streamed import or build response the recorder scans.
	maxStreamLine = 1 << 20
	// namedVerbSegments is /<kind>/<id-or-name>/<verb>, split on its slashes.
	namedVerbSegments = 3
	// createSegment is the path segment of every create.
	createSegment = "create"
	// imagesSegment is the first path segment of every image call, and imagesPath prefixes its path.
	imagesSegment = "images"
	imagesPath    = "/" + imagesSegment + "/"
)

var (
	// versionPrefix is the API version a client may prefix a path with.
	versionPrefix = regexp.MustCompile(`^/v\d+\.\d+(/|$)`)
	// prunePath is every Engine API path that removes by filter.
	prunePath = regexp.MustCompile(`^/(containers|images|volumes|networks|build)/prune$`)
	// errRecorder means a recorded invocation broke one of the audit's conditions.
	errRecorder = errors.New("recorder audit")
)

// targetQueries are the only query values a recorded call keeps: the ones that name what the call
// acts on. Everything else a client sends stays out of the log.
func targetQueries() []string {
	return []string{"name", "repo", "tag", "fromImage", "fromSrc", "container", "path", "t"}
}

// Call is one Engine API call the recorder forwarded.
type Call struct {
	// Query holds only the target-naming query values.
	Query url.Values
	// Method and Path are the request's, the API version prefix included.
	Method string
	Path   string
	// Created is the ID or name a create, commit, import or build returned.
	Created string
	// Name is the name a create asked for.
	Name string
	// Kind and Check are the create's Stutter kind and check labels.
	Kind  string
	Check string
	// Targets are what the call acts on, each of which must be the invocation's own.
	Targets []string
	Seq     int
	Status  int
	// Mutating, Prune and BuildChannel classify the call.
	Mutating     bool
	Prune        bool
	BuildChannel bool
}

// Created is what the recorded invocation created.
type Created struct {
	Containers []string
	Networks   []string
	Volumes    []string
	Images     []string
}

// Summary is the recorder's audit of one invocation.
type Summary struct {
	// Foreign holds every mutating call that touched something the invocation did not create.
	Foreign         []Call
	Calls           int
	Mutating        int
	Prune           int
	ForeignTouched  int
	InspectFailures int
}

// Recorder is an Engine API reverse proxy on a unix socket: point a client's DOCKER_HOST at Host and
// it records every call the client makes, then judges each by what the recorder saw created.
type Recorder struct {
	server   *http.Server
	upstream string
	socket   string
	calls    []Call
	mu       sync.Mutex
}

// callKey carries a request's index in the log from the handler to the response hook.
type callKey struct{}

// Recorder starts a recorder in front of this engine. It stops, and its socket goes, when tb ends; a
// server that stopped for any other reason fails tb then.
func (e Engine) Recorder(tb testing.TB) *Recorder {
	tb.Helper()

	//nolint:usetesting // a unix socket path must stay under 108 bytes, and a test's temp dir is longer.
	dir, err := os.MkdirTemp("", "rec")
	if err != nil {
		tb.Fatalf("making the recorder's directory: %v", err)
	}

	socket := filepath.Join(dir, "d.sock")
	if len(socket) > maxSocketPath {
		tb.Fatalf("the recorder's socket path %s is longer than a unix socket holds", socket)
	}

	listener, err := (&net.ListenConfig{}).Listen(tb.Context(), "unix", socket)
	if err != nil {
		tb.Fatalf("listening on %s: %v", socket, err)
	}

	r := &Recorder{upstream: strings.TrimPrefix(e.Endpoint(), "unix://"), socket: socket}
	r.server = &http.Server{Handler: r.handler(r.proxy()), ReadHeaderTimeout: headerTimeout}
	served := make(chan error, 1)

	go func() { served <- r.server.Serve(listener) }()

	tb.Cleanup(func() {
		if err := r.server.Close(); err != nil {
			tb.Errorf("closing the recorder: %v", err)
		}

		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			tb.Errorf("the recorder stopped serving: %v", err)
		}

		if err := os.RemoveAll(dir); err != nil {
			tb.Errorf("removing the recorder's directory: %v", err)
		}
	})

	return r
}

// DockerVia returns a test-decoy helper whose calls go through the recorder.
func (e Engine) DockerVia(tb testing.TB, r *Recorder) *Docker {
	tb.Helper()

	return &Docker{engine: e, host: r.Host(), config: tb.TempDir()}
}

// Host is the recorder's endpoint, as DOCKER_HOST spells it.
func (r *Recorder) Host() string {
	return "unix://" + r.socket
}

// Calls returns every call recorded so far, in arrival order.
func (r *Recorder) Calls() []Call {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.calls)
}

// Created returns what the recorded calls created.
func (r *Recorder) Created() Created {
	var c Created

	lists := map[Object]*[]string{
		ObjectContainer: &c.Containers, ObjectNetwork: &c.Networks, ObjectVolume: &c.Volumes, ObjectImage: &c.Images,
	}

	for _, call := range r.Calls() {
		if list, ok := lists[classify(call.Method, call.Path, call.Query).creates]; ok && call.Created != "" {
			*list = append(*list, call.Created)
		}
	}

	return c
}

// Summary audits the recorded calls for the check checkID: an image reference under that check's
// scheme is its own, as is anything the recorder saw it create.
func (r *Recorder) Summary(checkID string) Summary {
	return summarize(r.Calls(), checkID)
}

// Line formats the summary as its audit line.
func (s Summary) Line(checkID string) string {
	line, err := testgate.FormatAudit(testgate.AuditRecorder, checkID, s.Calls, s.Mutating, s.Prune,
		s.ForeignTouched)
	if err != nil {
		return testgate.AuditPrefix + " recorder " + err.Error()
	}

	return line
}

// Err reports every condition the invocation broke: no call reached the recorder, a prune, a
// mutating call on something foreign, or a container the recorder could not read back.
func (s Summary) Err() error {
	var problems []string

	if s.Calls == 0 {
		problems = append(problems, "no call reached the recorder: the invocation bypassed it")
	}

	if s.Prune > 0 {
		problems = append(problems, fmt.Sprintf("%d prune calls", s.Prune))
	}

	for _, c := range s.Foreign {
		problems = append(problems, fmt.Sprintf("call %d %s %s touched %q, which the invocation did not create",
			c.Seq, c.Method, c.Path, c.Targets))
	}

	if s.InspectFailures > 0 {
		problems = append(problems, fmt.Sprintf("%d containers could not be read back", s.InspectFailures))
	}

	if problems == nil {
		return nil
	}

	return fmt.Errorf("%w: %s", errRecorder, strings.Join(problems, "; "))
}

// proxy returns the reverse proxy to the engine's socket: responses flushed as they arrive, protocol
// upgrades passed through.
func (r *Recorder) proxy() *httputil.ReverseProxy {
	upstream := r.upstream

	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme, pr.Out.URL.Host, pr.Out.Host = "http", "docker", "docker"
		},
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", upstream)
			},
			DisableCompression: true,
		},
		FlushInterval:  -1,
		ModifyResponse: r.response,
		ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
			if i, ok := req.Context().Value(callKey{}).(int); ok {
				r.update(i, func(c *Call) { c.Status = http.StatusBadGateway })
			}

			http.Error(w, "recorder: "+err.Error(), http.StatusBadGateway)
		},
	}
}

// handler records each request before it is forwarded.
func (r *Recorder) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		i := r.begin(req)
		next.ServeHTTP(w, req.WithContext(context.WithValue(req.Context(), callKey{}, i)))
	})
}

// begin records a request and returns its index in the log.
func (r *Recorder) begin(req *http.Request) int {
	cl := classify(req.Method, req.URL.Path, req.URL.Query())
	c := Call{
		Method: req.Method, Path: req.URL.Path, Query: keep(req.URL.Query()), Targets: cl.targets,
		Mutating: cl.mutating, Prune: cl.prune, BuildChannel: cl.buildChannel,
	}

	if cl.creates == ObjectContainer || cl.creates == ObjectNetwork || cl.creates == ObjectVolume || cl.connect {
		readCreate(req, cl, &c)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	c.Seq = len(r.calls) + 1
	r.calls = append(r.calls, c)

	return len(r.calls) - 1
}

// createBody is what the recorder keeps of a create or connect request's body.
type createBody struct {
	Labels    map[string]string `json:"Labels"`    //nolint:tagliatelle // the engine's own field name
	Name      string            `json:"Name"`      //nolint:tagliatelle // the engine's own field name
	Container string            `json:"Container"` //nolint:tagliatelle // the engine's own field name
}

// readCreate reads a create's or a network connect's body into c, keeping only labels and names, and
// restores the body for the engine. A create body also holds the environment, which is never kept.
func readCreate(req *http.Request, cl callClass, c *Call) {
	var body createBody

	if raw, err := readBack(&req.Body); err == nil && json.Unmarshal(raw, &body) == nil {
		c.Kind, c.Check, c.Name = body.Labels[rules.LabelKind], body.Labels[rules.LabelCheck], body.Name

		if cl.connect && body.Container != "" {
			c.Targets = append(c.Targets, body.Container)
		}
	}

	if name := req.URL.Query().Get("name"); cl.creates == ObjectContainer && name != "" {
		c.Name = name
	}
}

// response records a response's status, and what a create, commit, import or build returned.
func (r *Recorder) response(resp *http.Response) error {
	i, ok := resp.Request.Context().Value(callKey{}).(int)
	if !ok {
		return nil
	}

	r.update(i, func(c *Call) { c.Status = resp.StatusCode })

	if resp.StatusCode >= http.StatusMultipleChoices {
		return nil
	}

	cl := classify(resp.Request.Method, resp.Request.URL.Path, resp.Request.URL.Query())

	switch {
	case cl.streamsID:
		resp.Body = &idScanner{body: resp.Body, found: func(id string) {
			r.update(i, func(c *Call) { c.Created = id })
		}}
	case cl.creates != "":
		var created createdBody

		if raw, err := readBack(&resp.Body); err == nil && json.Unmarshal(raw, &created) == nil {
			r.update(i, func(c *Call) { c.Created = cmpOr(created.ID, created.Name) })
		}
	default:
	}

	return nil
}

// createdBody is what a create or commit response names.
type createdBody struct {
	ID   string `json:"Id"`   //nolint:tagliatelle // the engine's own field name
	Name string `json:"Name"` //nolint:tagliatelle // the engine's own field name
}

// update changes the logged call at index i.
func (r *Recorder) update(i int, change func(*Call)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	change(&r.calls[i])
}

// readBack reads a body up to maxBody and puts what it read back in front of the rest.
func readBack(body *io.ReadCloser) ([]byte, error) {
	if *body == nil || *body == http.NoBody {
		return nil, nil
	}

	raw, err := io.ReadAll(io.LimitReader(*body, maxBody))
	*body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(raw), *body), *body}

	if err != nil {
		return raw, fmt.Errorf("reading a body: %w", err)
	}

	return raw, nil
}

// keep returns only the target-naming query values.
func keep(query url.Values) url.Values {
	kept := url.Values{}

	for _, key := range targetQueries() {
		if values, ok := query[key]; ok {
			kept[key] = values
		}
	}

	return kept
}

// cmpOr returns the first non-empty string.
func cmpOr(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}

	return ""
}

// idScanner passes a streamed import or build response through, and reports the image ID it names
// on its way past: an import's final sha256: status, a build's aux ID.
type idScanner struct {
	body  io.ReadCloser
	found func(id string)
	line  []byte
}

// streamAux is the aux part of one streamed message.
type streamAux struct {
	ID string `json:"ID"` //nolint:tagliatelle // the engine's own field name
}

// streamMessage is one message of a streamed import or build response.
type streamMessage struct {
	Status string    `json:"status"`
	Aux    streamAux `json:"aux"`
}

// Read reads the body and scans what it read.
func (s *idScanner) Read(p []byte) (int, error) {
	n, err := s.body.Read(p)

	for _, b := range p[:n] {
		if b != '\n' {
			if len(s.line) < maxStreamLine {
				s.line = append(s.line, b)
			}

			continue
		}

		s.scanLine()
	}

	return n, err //nolint:wrapcheck // a pass-through reader returns its body's errors as they are
}

// Close scans a final unterminated line and closes the body.
func (s *idScanner) Close() error {
	s.scanLine()

	return s.body.Close() //nolint:wrapcheck // a pass-through reader returns its body's errors as they are
}

// scanLine reports the ID the buffered line names, if any, and empties the buffer.
func (s *idScanner) scanLine() {
	var message streamMessage

	if json.Unmarshal(bytes.TrimSpace(s.line), &message) == nil {
		switch {
		case strings.HasPrefix(message.Status, "sha256:"):
			s.found(message.Status)
		case message.Aux.ID != "":
			s.found(message.Aux.ID)
		default:
		}
	}

	s.line = s.line[:0]
}

// callClass is what one Engine API call is, for the audit.
type callClass struct {
	// creates is the kind of object the call creates, if it creates one.
	creates Object
	// targets are what the call acts on, each of which must be the invocation's own.
	targets []string
	// mutating calls change the engine; prune calls remove by filter; build-channel calls carry a
	// BuildKit session, whose tags the tags scan covers.
	mutating     bool
	prune        bool
	buildChannel bool
	// pull is own only when this recorder saw the image inspected absent first.
	pull bool
	// foreign calls are never the invocation's own, whatever they name.
	foreign bool
	// connect calls name a second target, a container, in their body.
	connect bool
	// streamsID calls stream the new image's ID back in their response.
	streamsID bool
}

// classify classifies one call by its method, path and query. A read, and a mutating call on a
// named target, are told apart by the path; any mutating call it does not know is foreign.
func classify(method, path string, query url.Values) callClass {
	p := versionPrefix.ReplaceAllString(path, "/")

	if cl, ok := classifyAnyPath(method, p); ok {
		return cl
	}

	segs := strings.Split(strings.TrimPrefix(p, "/"), "/")

	switch segs[0] {
	case "containers":
		return classifyContainer(method, segs)
	case "networks":
		return classifyNetwork(method, segs)
	case "volumes":
		if cl, ok := createOrDelete(method, segs, ObjectVolume); ok {
			return cl
		}

		return foreignCall()
	case imagesSegment:
		return classifyImage(method, strings.TrimPrefix(p, imagesPath), query)
	case "commit":
		return callClass{
			mutating: method == http.MethodPost, creates: ObjectImage,
			targets: nonEmpty(query.Get("container"), reference(query.Get("repo"), query.Get("tag"))),
		}
	case "build":
		return callClass{mutating: true, creates: ObjectImage, streamsID: true, targets: query["t"]}
	default:
		return foreignCall()
	}
}

// classifyAnyPath classifies what the method or path alone decides: a read, the build channel, a
// prune. ok is false for anything else.
func classifyAnyPath(method, p string) (callClass, bool) {
	switch {
	case method == http.MethodGet || method == http.MethodHead:
		return callClass{}, true
	case method == http.MethodPost && (p == "/grpc" || p == "/session"):
		return callClass{buildChannel: true}, true
	case prunePath.MatchString(p):
		return callClass{mutating: true, prune: true, foreign: true}, true
	default:
		return callClass{}, false
	}
}

// foreignCall is a mutating call that is never the invocation's own.
func foreignCall() callClass {
	return callClass{mutating: true, foreign: true}
}

// createOrDelete classifies /<kind>/create and a delete of /<kind>/<id-or-name>. ok is false for
// anything else.
func createOrDelete(method string, segs []string, kind Object) (callClass, bool) {
	switch {
	case len(segs) == 2 && segs[1] == createSegment && method == http.MethodPost:
		return callClass{mutating: true, creates: kind}, true
	case len(segs) == 2 && method == http.MethodDelete:
		return callClass{mutating: true, targets: segs[1:2]}, true
	default:
		return callClass{}, false
	}
}

// classifyContainer classifies /containers/create and /containers/<id-or-name>[/<verb>].
func classifyContainer(method string, segs []string) callClass {
	if cl, ok := createOrDelete(method, segs, ObjectContainer); ok {
		return cl
	}

	switch {
	case len(segs) != namedVerbSegments:
		return foreignCall()
	case method == http.MethodPost && slices.Contains(containerReads(), segs[2]):
		return callClass{}
	case method == http.MethodPost, method == http.MethodPut && segs[2] == "archive":
		return callClass{mutating: true, targets: segs[1:2]}
	default:
		return foreignCall()
	}
}

// classifyNetwork classifies /networks/create, a delete, and a connect or disconnect.
func classifyNetwork(method string, segs []string) callClass {
	if cl, ok := createOrDelete(method, segs, ObjectNetwork); ok {
		return cl
	}

	if len(segs) == namedVerbSegments && method == http.MethodPost &&
		(segs[2] == "connect" || segs[2] == "disconnect") {
		return callClass{mutating: true, targets: segs[1:2], connect: true}
	}

	return foreignCall()
}

// containerReads are the container POST verbs that change nothing.
func containerReads() []string {
	return []string{"wait", "attach", "resize"}
}

// classifyImage classifies a call under /images/: rest is the path after that prefix, and an image
// reference in it may itself hold slashes.
func classifyImage(method, rest string, query url.Values) callClass {
	switch {
	case method == http.MethodPost && rest == createSegment && query.Has("fromSrc"):
		return callClass{
			mutating: true, creates: ObjectImage, streamsID: true,
			targets: nonEmpty(reference(query.Get("repo"), query.Get("tag"))),
		}
	case method == http.MethodPost && rest == createSegment:
		return callClass{
			mutating: true, pull: true,
			targets: []string{familiar(reference(query.Get("fromImage"), query.Get("tag")))},
		}
	case method == http.MethodPost && strings.HasSuffix(rest, "/tag"):
		return callClass{
			mutating: true,
			targets:  nonEmpty(strings.TrimSuffix(rest, "/tag"), reference(query.Get("repo"), query.Get("tag"))),
		}
	case method == http.MethodDelete:
		return callClass{mutating: true, targets: []string{rest}}
	default:
		return foreignCall()
	}
}

// reference joins a repository and a tag; the repository alone when there is no tag.
func reference(repo, tag string) string {
	if repo == "" || tag == "" {
		return repo
	}

	return repo + ":" + tag
}

// familiar spells an image reference the short way the CLI does: no default registry or library
// prefix, and :latest when it has no tag or digest.
func familiar(ref string) string {
	defaults := []string{"docker.io/library/", "index.docker.io/library/", "docker.io/", "index.docker.io/"}
	for _, prefix := range defaults {
		ref = strings.TrimPrefix(ref, prefix)
	}

	if last := ref[strings.LastIndexByte(ref, '/')+1:]; ref != "" && !strings.Contains(last, ":") &&
		!strings.Contains(ref, "@") {
		ref += ":latest"
	}

	return ref
}

// nonEmpty returns values without the empty ones.
func nonEmpty(values ...string) []string {
	return slices.DeleteFunc(values, func(v string) bool { return v == "" })
}

// inspectedImage returns the image an image inspect names.
func inspectedImage(c Call) (string, bool) {
	p := versionPrefix.ReplaceAllString(c.Path, "/")
	if c.Method != http.MethodGet || !strings.HasPrefix(p, imagesPath) || !strings.HasSuffix(p, "/json") {
		return "", false
	}

	return strings.TrimSuffix(strings.TrimPrefix(p, imagesPath), "/json"), true
}

// ownership is what an audit has seen created, and which images it saw inspected absent.
type ownership struct {
	created map[string]bool
	absent  map[string]bool
	scheme  string
}

// own reports whether target is the invocation's: seen created, by exact ID or name, or an image
// reference under the check's scheme. A prefix of an ID is never enough.
func (o ownership) own(target string) bool {
	target = strings.TrimPrefix(target, "/")

	return o.created[target] || strings.HasPrefix(target, o.scheme)
}

// foreign reports whether a mutating call touched anything not the invocation's, and what it named.
func (o ownership) foreign(c Call, cl callClass) ([]string, bool) {
	targets := c.Targets
	if len(targets) == 0 {
		targets = cl.targets
	}

	switch {
	case cl.foreign:
		return targets, true
	case cl.pull:
		return targets, !o.absent[targets[0]]
	default:
		return targets, slices.ContainsFunc(targets, func(t string) bool { return !o.own(t) })
	}
}

// saw records what an own call created or named.
func (o ownership) saw(c Call, cl callClass, targets []string) {
	named := slices.Concat(nonEmpty(c.Created, c.Name), targets)
	if cl.creates == ObjectContainer {
		named = append(named, c.Query.Get("name"))
	}

	for _, name := range named {
		if name = strings.TrimPrefix(name, "/"); name != "" {
			o.created[name] = true
		}
	}
}

// summarize audits calls, in arrival order, for the check checkID.
func summarize(calls []Call, checkID string) Summary {
	s := Summary{Calls: len(calls)}
	o := ownership{
		created: map[string]bool{}, absent: map[string]bool{}, scheme: rules.ImageDomain + "/" + checkID + "/",
	}

	for _, c := range slices.SortedFunc(slices.Values(calls), func(a, b Call) int { return a.Seq - b.Seq }) {
		cl := classify(c.Method, c.Path, c.Query)

		if !cl.mutating {
			if image, ok := inspectedImage(c); ok && c.Status == http.StatusNotFound {
				o.absent[familiar(image)] = true
			}

			continue
		}

		s.Mutating++

		if cl.prune {
			s.Prune++
		}

		targets, foreign := o.foreign(c, cl)
		if foreign {
			c.Targets = targets
			s.ForeignTouched++
			s.Foreign = append(s.Foreign, c)

			continue
		}

		o.saw(c, cl, targets)
	}

	return s
}
