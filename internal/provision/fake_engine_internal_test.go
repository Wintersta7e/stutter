//go:build linux

package provision

import (
	"context"
	"encoding/json"
	"fmt"
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
	labels   map[string]string
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
	argv       []string
}

// fakeEngine is a stateful engine: networks with IPAM, volumes, containers and images, each with
// labels. It records every call in order, and can be told to refuse calls or to answer as a
// foreign engine would.
type fakeEngine struct {
	objects map[ResourceType]map[string]*fakeObject
	// afterCreate, when set, edits a network or volume as the engine reports it after create.
	afterCreate func(typ ResourceType, obj *fakeObject)
	logCall     func(callLine)
	ledgerPath  string
	configDir   string
	calls       []fakeCall
	nextID      int
	mu          sync.Mutex
	unreachable bool
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{objects: map[ResourceType]map[string]*fakeObject{
		ResourceContainer: {}, ResourceNetwork: {}, ResourceVolume: {}, ResourceImage: {},
	}}
}

func (f *fakeEngine) attach(configDir string, logCall func(callLine)) {
	f.configDir, f.logCall = configDir, logCall
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

func (f *fakeEngine) call(_ context.Context, req request) (result, error) {
	spec := verbs()[req.verb]

	typed, err := spec.tokens(req)
	if err != nil {
		return result{}, err
	}

	argv := make([]string, 0, len(typed))
	for _, a := range typed {
		argv = append(argv, a.val)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, fakeCall{verb: spec.name, argv: argv, lastLedger: f.lastLedgerLine()})

	if f.unreachable {
		return fail(spec, "Cannot connect to the Docker daemon")
	}

	rest := argv[len(spec.prefix):]

	return f.answer(req.verb, spec, rest)
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
		verbNetworkList:   func() (result, error) { return f.list(ResourceNetwork, rest[1:]) },
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

func (f *fakeEngine) list(typ ResourceType, rest []string) (result, error) {
	var out strings.Builder

	for _, obj := range f.objects[typ] {
		if len(rest) == 2 && rest[0] == "--filter" {
			key, value, hasValue := strings.Cut(strings.TrimPrefix(rest[1], "label="), "=")
			if got, ok := obj.labels[key]; !ok || hasValue && got != value {
				continue
			}
		}

		out.WriteString(obj.id + "\n")
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

		line, err := json.Marshal(obj.report(typ))
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

	engine, err := openWith(t.Context(), Options{StateDir: t.TempDir() + "/state", TempDir: t.TempDir()}, openDeps{
		admit: func(context.Context) (Identity, engineCaller, error) { return testIdentity(), fake, nil },
		host:  defaultHostFS(),
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

	return engine, fake
}

// ourLabels is the label set this engine gives a resource of kind.
func ourLabels(e *Engine, kind rules.Kind) map[string]string {
	return labelSet(e.id, kind, "")
}
