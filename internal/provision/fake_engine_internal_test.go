//go:build linux

package provision

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/netip"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// fakeObject is one resource the fake engine holds.
type fakeObject struct {
	labels  map[string]string
	volumes map[string]any
	// inspect is a created container's whole inspect, in the container template's shape.
	inspect *containerReport
	// stopped is closed when a started container stops.
	stopped chan struct{}
	// output and stderr are what the container wrote to each stream.
	output   string
	stderr   string
	env      []string
	anon     []string
	copied   []byte
	id       string
	name     string
	subnet   string
	gateway  string
	tags     []string
	internal bool
	ipv6     bool
}

// fakeCall is one call the fake engine answered: its verb, its argv after the program, and the
// ledger's last line when it arrived.
type fakeCall struct {
	verb       string
	lastLedger string
	stdin      string
	argv       []string
	env        []string
}

// fakeEngine is a stateful engine: networks with IPAM, volumes, containers and images, each with
// labels. It records every call in order, and can be told to refuse calls or to answer as a
// foreign engine would.
type fakeEngine struct {
	objects map[ResourceType]map[string]*fakeObject
	// afterCreate, when set, edits a network or volume as the engine reports it after create.
	afterCreate func(typ ResourceType, obj *fakeObject)
	// hang names verbs whose calls block until their context ends, as a hung engine does.
	hang    map[string]bool
	logCall func(callLine)
	// inspectHook, when set, edits every created container's inspect as the engine reports it.
	inspectHook func(inspect *containerReport)
	// registry holds the images a pull can land, by reference.
	registry    map[string]*fakeObject
	ledgerPath  string
	configDir   string
	calls       []fakeCall
	nextID      int
	mu          sync.Mutex
	unreachable bool
	// readOnly refuses mutating calls, as a runner attached read-only does.
	readOnly bool
	// buildDropsLabels makes a build land images without the labels it was handed.
	buildDropsLabels bool
	// gracefulExit is the exit code a graceful stop leaves: 137 when the grace period ran out.
	gracefulExit int
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{objects: map[ResourceType]map[string]*fakeObject{
		ResourceContainer: {}, ResourceNetwork: {}, ResourceVolume: {}, ResourceImage: {},
	}}
}

func (f *fakeEngine) attach(configDir string, logCall func(callLine), mutable bool) {
	f.configDir, f.logCall, f.readOnly = configDir, logCall, !mutable
}

// add puts an object into the engine as another party would, returning its ID.
func (f *fakeEngine) add(typ ResourceType, obj *fakeObject) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	if obj.id == "" {
		obj.id = f.mintID()
	}

	f.objects[typ][obj.id] = obj

	return obj.id
}

func (f *fakeEngine) mintID() string {
	f.nextID++

	return fmt.Sprintf("%064x", f.nextID)
}

// find resolves ref as the engine does: by ID, then by name, then by an image tag.
func (f *fakeEngine) find(typ ResourceType, ref string) *fakeObject {
	if ref == "" {
		return nil
	}

	if obj, ok := f.objects[typ][ref]; ok {
		return obj
	}

	for _, obj := range f.objects[typ] {
		if obj.name == ref || slices.Contains(obj.tags, ref) {
			return obj
		}
	}

	return nil
}

// verbCalls returns the argv of every call of the named verb.
func (f *fakeEngine) verbCalls(name string) [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out [][]string

	for _, c := range f.calls {
		if c.verb == name {
			out = append(out, c.argv)
		}
	}

	return out
}

func (f *fakeEngine) call(ctx context.Context, req request) (result, error) {
	spec := verbs()[req.verb]

	typed, err := spec.tokens(req)
	if err != nil {
		return result{}, err
	}

	argv := make([]string, 0, len(typed))
	for _, a := range typed {
		argv = append(argv, a.val)
	}

	var stdin []byte

	if req.stdin != nil {
		if stdin, err = io.ReadAll(req.stdin); err != nil {
			return result{}, err
		}

		req.stdin = bytes.NewReader(stdin)
	}

	f.mu.Lock()
	f.calls = append(f.calls, fakeCall{
		verb: spec.name, argv: argv, lastLedger: f.lastLedgerLine(), stdin: string(stdin), env: req.extraEnv,
	})
	hung := f.hang[spec.name]
	f.mu.Unlock()

	if hung {
		<-ctx.Done()

		return result{exit: -1}, fmt.Errorf("%w: %s call exceeded its deadline", ErrDeadline, spec.name)
	}

	if req.verb == verbWait {
		return f.wait(ctx, argv[len(argv)-1])
	}

	if err := ctx.Err(); err != nil {
		return result{exit: -1}, fmt.Errorf("%s call cancelled: %w", spec.name, err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.readOnly && spec.mutates {
		return result{}, fmt.Errorf("%w: the %s call mutates the engine, and this runner is read-only",
			ErrEngine, spec.name)
	}

	if f.unreachable {
		return fail(spec, "Cannot connect to the Docker daemon")
	}

	rest := argv[len(spec.prefix):]

	if answer, ok := f.imageAnswers(req, spec, rest); ok {
		return answer()
	}

	return f.answer(req.verb, spec, rest)
}

// imageAnswers answers the calls that make images: pull, build, import and commit.
func (f *fakeEngine) imageAnswers(req request, spec verbSpec, rest []string) (func() (result, error), bool) {
	answers := map[verb]func() (result, error){
		verbPull:         func() (result, error) { return f.pull(spec, rest[len(rest)-1]) },
		verbComposeBuild: func() (result, error) { return f.build(req.stdin) },
		verbImport:       func() (result, error) { return f.importImage(rest) },
		verbCommit:       func() (result, error) { return f.commit(spec, rest) },
		verbCreate:       func() (result, error) { return f.create(req, rest) },
		verbCopyIn:       func() (result, error) { return f.copyIn(spec, req.stdin, rest) },
		verbCopyOut:      func() (result, error) { return f.copyOut(spec, rest) },
		verbStart:        func() (result, error) { return f.start(spec, rest) },
		verbStopGraceful: func() (result, error) { return f.stopGraceful(spec, rest) },
		verbLogs:         func() (result, error) { return f.logs(spec, req, rest) },
		verbLogsFollow:   func() (result, error) { return f.logs(spec, req, rest) },
	}

	answer, ok := answers[req.verb]

	return answer, ok
}

// pull lands a registry image under ref, as a pull does.
func (f *fakeEngine) pull(spec verbSpec, ref string) (result, error) {
	image, ok := f.registry[ref]
	if !ok {
		return fail(spec, "pull access denied for "+ref)
	}

	pulled := *image
	pulled.tags = []string{ref}

	if existing := f.find(ResourceImage, image.id); existing != nil {
		existing.tags = append(existing.tags, ref)
	} else {
		f.objects[ResourceImage][pulled.id] = &pulled
	}

	return result{out: []byte(ref + "\n")}, nil
}

// fakeBuild is what a test's render function hands the fake's build: per service, a tag and labels.
type fakeBuild map[string]struct {
	Labels map[string]string `json:"labels"`
	Tag    string            `json:"tag"`
}

// build lands one image per service of the rendered model, dropping labels when told to.
func (f *fakeEngine) build(stdin io.Reader) (result, error) {
	var model fakeBuild
	if err := json.NewDecoder(stdin).Decode(&model); err != nil {
		return result{exit: 1}, &CallError{Verb: "composeBuild", Code: 1}
	}

	for _, service := range model {
		labels := service.Labels
		if f.buildDropsLabels {
			labels = map[string]string{}
		}

		id := "sha256:" + f.mintID()
		f.objects[ResourceImage][id] = &fakeObject{id: id, tags: []string{service.Tag}, labels: labels}
	}

	return result{}, nil
}

// changes reads `--change 'LABEL k="v"'` arguments into labels.
func changes(args []string) map[string]string {
	labels := map[string]string{}

	for i := 0; i+1 < len(args); i++ {
		if args[i] != "--change" {
			continue
		}

		key, value, ok := strings.Cut(strings.TrimPrefix(args[i+1], "LABEL "), "=")
		if ok {
			labels[key] = strings.Trim(value, `"`)
		}
	}

	return labels
}

// importImage lands an image whose labels come from its --change lines, tagged with the last argument.
func (f *fakeEngine) importImage(rest []string) (result, error) {
	id := "sha256:" + f.mintID()
	f.objects[ResourceImage][id] = &fakeObject{id: id, tags: []string{rest[len(rest)-1]}, labels: changes(rest)}

	return result{out: []byte(id + "\n")}, nil
}

// commit lands an image from a container: its labels, overridden by the --change lines.
func (f *fakeEngine) commit(spec verbSpec, rest []string) (result, error) {
	container := f.find(ResourceContainer, rest[len(rest)-2])
	if container == nil {
		return fail(spec, "No such container")
	}

	labels := map[string]string{}
	maps.Copy(labels, container.labels)
	maps.Copy(labels, changes(rest))

	id := "sha256:" + f.mintID()
	f.objects[ResourceImage][id] = &fakeObject{id: id, tags: []string{rest[len(rest)-1]}, labels: labels}

	return result{out: []byte(id + "\n")}, nil
}

func (f *fakeEngine) answer(v verb, spec verbSpec, rest []string) (result, error) {
	reads := map[verb]ResourceType{
		verbInspect: ResourceContainer, verbNetworkInspect: ResourceNetwork, verbVolumeInspect: ResourceVolume,
		verbImageInspect: ResourceImage,
	}
	removes := map[verb]ResourceType{
		verbRemove: ResourceContainer, verbNetworkRemove: ResourceNetwork, verbVolumeRemove: ResourceVolume,
		verbImageRemove: ResourceImage,
	}

	if typ, ok := reads[v]; ok {
		return f.inspect(spec, typ, rest[1:])
	}

	if typ, ok := removes[v]; ok {
		return f.remove(spec, typ, rest)
	}

	answers := map[verb]func() (result, error){
		verbVersion:       func() (result, error) { return result{out: []byte(healthyVersion)}, nil },
		verbNetworkCreate: func() (result, error) { return f.createNetwork(spec, rest) },
		verbVolumeCreate:  func() (result, error) { return f.createVolume(rest) },
		verbNetworkList:   func() (result, error) { return f.list(ResourceNetwork, rest) },
		verbContainerList: func() (result, error) { return f.list(ResourceContainer, rest) },
		verbVolumeList:    func() (result, error) { return f.list(ResourceVolume, rest) },
		verbImageList:     func() (result, error) { return f.list(ResourceImage, rest) },
		verbKill:          func() (result, error) { return f.kill(spec, rest) },
	}

	if answer, ok := answers[v]; ok {
		return answer()
	}

	return fail(spec, "the fake engine does not answer "+spec.name)
}

func fail(spec verbSpec, stderr string) (result, error) {
	return result{exit: 1, stderrFirst: stderr}, &CallError{Verb: spec.name, Code: 1, Stderr: stderr}
}

// parseCreate reads `--label k=v`, `--internal`, `--subnet X` and the trailing name.
func parseCreate(rest []string) (*fakeObject, string) {
	obj := &fakeObject{labels: map[string]string{}}
	subnet := ""

	for i := 0; i < len(rest)-1; i++ {
		switch rest[i] {
		case "--label":
			key, value, _ := strings.Cut(rest[i+1], "=")
			obj.labels[key] = value
			i++
		case "--internal":
			obj.internal = true
		case "--subnet":
			subnet = rest[i+1]
			i++
		default:
		}
	}

	obj.name = rest[len(rest)-1]

	return obj, subnet
}

func (f *fakeEngine) createNetwork(spec verbSpec, rest []string) (result, error) {
	obj, subnet := parseCreate(rest)

	requested, err := netip.ParsePrefix(subnet)
	if err != nil {
		return fail(spec, "invalid subnet")
	}

	for _, other := range f.objects[ResourceNetwork] {
		if held, err := netip.ParsePrefix(other.subnet); err == nil && held.Overlaps(requested) {
			return fail(spec, "Pool overlaps with other one on this address space")
		}
	}

	obj.id, obj.subnet = f.mintID(), requested.String()
	obj.gateway = requested.Addr().Next().String()

	if f.afterCreate != nil {
		f.afterCreate(ResourceNetwork, obj)
	}

	f.objects[ResourceNetwork][obj.id] = obj

	return result{out: []byte(obj.id + "\n")}, nil
}

// createVolume returns an existing volume of that name untouched, as the engine does.
func (f *fakeEngine) createVolume(rest []string) (result, error) {
	obj, _ := parseCreate(rest)

	if existing := f.find(ResourceVolume, obj.name); existing != nil {
		return result{out: []byte(existing.name + "\n")}, nil
	}

	obj.id = obj.name

	if f.afterCreate != nil {
		f.afterCreate(ResourceVolume, obj)
	}

	f.objects[ResourceVolume][obj.id] = obj

	return result{out: []byte(obj.name + "\n")}, nil
}

// list answers a listing: IDs for the ID template, else one JSON line per object in the shape the
// listing templates build; `--filter label=key[=value]` is honoured.
func (f *fakeEngine) list(typ ResourceType, rest []string) (result, error) {
	var out strings.Builder

	for _, obj := range f.objects[typ] {
		if len(rest) == 3 && rest[1] == "--filter" {
			key, value, hasValue := strings.Cut(strings.TrimPrefix(rest[2], "label="), "=")
			if got, ok := obj.labels[key]; !ok || hasValue && got != value {
				continue
			}
		}

		if rest[0] == idTemplate {
			out.WriteString(obj.id + "\n")

			continue
		}

		line := map[string]string{
			"id": obj.id, "name": obj.name, "check": obj.labels[rules.LabelCheck], "kind": obj.labels[rules.LabelKind],
		}

		if typ == ResourceImage {
			repo, tag, _ := strings.Cut(obj.tags[0], ":")
			line = map[string]string{"id": obj.id, "name": repo, "tag": tag}
		}

		encoded, err := json.Marshal(line)
		if err != nil {
			return result{}, err
		}

		out.Write(append(encoded, '\n'))
	}

	return result{out: []byte(out.String())}, nil
}

func (f *fakeEngine) inspect(spec verbSpec, typ ResourceType, refs []string) (result, error) {
	var out strings.Builder

	for _, ref := range refs {
		obj := f.find(typ, ref)
		if obj == nil {
			return fail(spec, "No such object: "+ref)
		}

		var answer any = obj.report(typ)

		if obj.inspect != nil {
			report := cloneReport(*obj.inspect)
			if f.inspectHook != nil {
				f.inspectHook(&report)
			}

			answer = report
		}

		line, err := json.Marshal(answer)
		if err != nil {
			return result{}, err
		}

		out.Write(append(line, '\n'))
	}

	return result{out: []byte(out.String())}, nil
}

// report is the object in the shape the driver's templates build.
func (o *fakeObject) report(typ ResourceType) map[string]any {
	out := map[string]any{"labels": o.labels}

	if typ == ResourceVolume {
		out["name"] = o.name

		return out
	}

	out["id"] = o.id

	if typ == ResourceImage {
		out["env"], out["entrypoint"], out["cmd"] = o.env, []string{"entry"}, []string{"run"}
		out["exposed"], out["volumes"] = map[string]any{"5432/tcp": map[string]any{}}, o.volumes
		out["os"], out["arch"] = "linux", "amd64"
	}

	if typ == ResourceNetwork {
		out["name"], out["internal"], out["ipv6"] = o.name, o.internal, o.ipv6
		out["subnets"], out["gateways"], out["members"] = []string{o.subnet}, []string{o.gateway}, []string{}
	}

	return out
}

func (f *fakeEngine) remove(spec verbSpec, typ ResourceType, refs []string) (result, error) {
	for _, ref := range refs {
		obj := f.find(typ, ref)
		if obj == nil {
			return fail(spec, "No such object: "+ref)
		}

		if typ == ResourceImage && len(obj.tags) > 1 {
			obj.tags = slices.DeleteFunc(obj.tags, func(tag string) bool { return tag == ref })

			continue
		}

		delete(f.objects[typ], obj.id)

		// `rm -v` takes a container's anonymous volumes with it.
		for _, volume := range obj.anon {
			delete(f.objects[ResourceVolume], volume)
		}
	}

	return result{}, nil
}

// kill stops a container; a stopped container stays, as the engine keeps it.
func (f *fakeEngine) kill(spec verbSpec, refs []string) (result, error) {
	for _, ref := range refs {
		container := f.find(ResourceContainer, ref)
		if container == nil {
			return fail(spec, "No such container: "+ref)
		}

		stopContainer(container, killedExit)
	}

	return result{}, nil
}

// removeVerb is the verb that removes a container.
const removeVerb = "remove"

// stopContainer ends a running container with exit, and releases its wait.
func stopContainer(container *fakeObject, exit int) {
	if container.inspect == nil || !container.inspect.Running {
		return
	}

	container.inspect.Running, container.inspect.ExitCode = false, exit

	if container.stopped != nil {
		close(container.stopped)
		container.stopped = nil
	}
}

// start runs a created container.
func (f *fakeEngine) start(spec verbSpec, rest []string) (result, error) {
	container := f.find(ResourceContainer, rest[len(rest)-1])
	if container == nil || container.inspect == nil {
		return fail(spec, "No such container")
	}

	container.inspect.Running, container.stopped = true, make(chan struct{})

	return result{}, nil
}

// stopGraceful stops a container as its stop signal would, leaving gracefulExit.
func (f *fakeEngine) stopGraceful(spec verbSpec, rest []string) (result, error) {
	container := f.find(ResourceContainer, rest[len(rest)-1])
	if container == nil || container.inspect == nil {
		return fail(spec, "No such container")
	}

	container.inspect.Running = true
	stopContainer(container, f.gracefulExit)

	return result{}, nil
}

// wait returns when the container stops, at once when it is not running, as the engine's does.
func (f *fakeEngine) wait(ctx context.Context, id string) (result, error) {
	f.mu.Lock()
	container := f.find(ResourceContainer, id)

	var stopped chan struct{}
	if container != nil {
		stopped = container.stopped
	}
	f.mu.Unlock()

	if stopped != nil {
		select {
		case <-stopped:
		case <-ctx.Done():
			return result{exit: -1}, fmt.Errorf("wait call cancelled: %w", ctx.Err())
		}
	}

	return result{out: []byte("0\n")}, nil
}

// logs writes what the container wrote, each stream to its own writer.
func (f *fakeEngine) logs(spec verbSpec, req request, rest []string) (result, error) {
	container := f.find(ResourceContainer, rest[len(rest)-1])
	if container == nil {
		return fail(spec, "No such container")
	}

	for _, stream := range []struct {
		to   io.Writer
		text string
	}{{req.stdout, container.output}, {req.stderr, container.stderr}} {
		if stream.to != nil {
			if _, err := io.WriteString(stream.to, stream.text); err != nil {
				return result{}, err
			}
		}
	}

	return result{}, nil
}

// lastLedgerLine is the ledger's last line when a call arrives.
func (f *fakeEngine) lastLedgerLine() string {
	if f.ledgerPath == "" {
		return ""
	}

	data, err := os.ReadFile(f.ledgerPath)
	if err != nil {
		return ""
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")

	return lines[len(lines)-1]
}

// openFakeEngine opens an Engine over a fresh fake engine with no local interfaces but loopback.
func openFakeEngine(t *testing.T) (*Engine, *fakeEngine) {
	t.Helper()

	fake := newFakeEngine()

	return openFakeEngineWith(t, t.TempDir()+"/state", fake), fake
}

// openFakeEngineWith opens an Engine over fake with its ledger in state, sweeping what is there.
func openFakeEngineWith(t *testing.T, state string, fake *fakeEngine) *Engine {
	t.Helper()

	return openFakeEngineHost(t, state, fake, defaultHostFS())
}

// openFakeEngineOpts opens an Engine over a fresh fake engine with the options given.
func openFakeEngineOpts(t *testing.T, opts Options) (*Engine, *fakeEngine) {
	t.Helper()

	fake := newFakeEngine()
	opts.StateDir = t.TempDir() + "/state"

	return openFake(t, opts, fake, defaultHostFS()), fake
}

// openFakeEngineHost is openFakeEngineWith on a host the test stands in for.
func openFakeEngineHost(t *testing.T, state string, fake *fakeEngine, host hostFS) *Engine {
	t.Helper()

	return openFake(t, Options{StateDir: state}, fake, host)
}

func openFake(t *testing.T, opts Options, fake *fakeEngine, host hostFS) *Engine {
	t.Helper()

	opts.TempDir = t.TempDir()

	engine, err := openWith(t.Context(), opts, openDeps{
		admit: func(context.Context) (Identity, engineCaller, error) { return testIdentity(), fake, nil },
		host:  host,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	fake.ledgerPath = engine.book.led.path
	engine.interfaces = func() ([]localAddr, error) {
		return []localAddr{{name: "lo", prefix: netip.MustParsePrefix("127.0.0.1/8")}}, nil
	}

	t.Cleanup(func() {
		if err := engine.release(); err != nil {
			t.Logf("release: %v", err)
		}
	})

	return engine
}

// ourLabels is the label set this engine gives a resource of kind.
func ourLabels(e *Engine, kind rules.Kind) map[string]string {
	return labelSet(e.id, kind, "")
}
