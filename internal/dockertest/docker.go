package dockertest

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

const (
	// callLimit bounds one docker call, a pull included.
	callLimit = 5 * time.Minute
	// subnetAttempts bounds how many test subnets CreateNetwork tries before it gives up.
	subnetAttempts = 8
	// testSubnetBits is the size of one test network's subnet.
	testSubnetBits = 24
	// flagAndValue is the number of argv elements one --label takes.
	flagAndValue = 2
)

// verbID names one row of the helper's verb table.
type verbID uint8

const (
	verbPull verbID = iota
	verbImport
	verbCreate
	verbStart
	verbKill
	verbWait
	verbNetworkCreate
	verbVolumeCreate
	verbCopyIn
	verbInspect
	verbImageInspect
	verbVolumeInspect
	verbNetworkInspect
	verbImages
	verbPs
	verbNetworkList
	verbVolumeList
	verbImagesLabelled
	verbPort
	verbInfo
	verbSave
	verbLoad
	verbRemove
	verbNetworkRemove
	verbVolumeRemove
	verbRemoveImage
)

// verb is one row of the helper's verb table: the fixed tokens every call of it starts with.
type verb struct {
	prefix  []string
	mutates bool
}

// decoyVerbs is every docker call a test can make. The table is closed: argv always starts with a
// row's fixed tokens, and nothing here runs, execs, tags, pushes, prunes or drives compose.
//
//nolint:goconst // the table spells every fixed token out, so it reads — and is audited — as the table it is.
func decoyVerbs() []verb {
	return []verb{
		verbPull:           {prefix: []string{"pull", "-q"}, mutates: true},
		verbImport:         {prefix: []string{"import"}, mutates: true},
		verbCreate:         {prefix: []string{"create", "--pull", "never"}, mutates: true},
		verbStart:          {prefix: []string{"start"}, mutates: true},
		verbKill:           {prefix: []string{"stop", "--signal", "KILL"}, mutates: true},
		verbWait:           {prefix: []string{"wait"}},
		verbNetworkCreate:  {prefix: []string{"network", "create"}, mutates: true},
		verbVolumeCreate:   {prefix: []string{"volume", "create"}, mutates: true},
		verbCopyIn:         {prefix: []string{"cp", "-"}, mutates: true},
		verbInspect:        {prefix: []string{"inspect", "--type", "container"}},
		verbImageInspect:   {prefix: []string{"image", "inspect"}},
		verbVolumeInspect:  {prefix: []string{"volume", "inspect"}},
		verbNetworkInspect: {prefix: []string{"network", "inspect"}},
		verbImages:         {prefix: []string{"images", "--no-trunc", "--format", "{{.Repository}}:{{.Tag}}\t{{.ID}}"}},
		verbPs:             {prefix: []string{"ps", "-a", "--no-trunc", "--format", "{{.ID}}", "--filter"}},
		verbNetworkList:    {prefix: []string{"network", "ls", "--no-trunc", "--format", "{{.ID}}", "--filter"}},
		verbVolumeList:     {prefix: []string{"volume", "ls", "--format", "{{.Name}}", "--filter"}},
		verbImagesLabelled: {prefix: []string{"images", "--no-trunc", "--format", "{{.ID}}", "--filter"}},
		verbPort:           {prefix: []string{"port"}},
		verbInfo: {prefix: []string{
			"info", "--format", "{{range .ClientInfo.Plugins}}{{.Name}}\t{{.Path}}\t{{.Version}}\n{{end}}",
		}},
		verbSave:          {prefix: []string{"save"}},
		verbLoad:          {prefix: []string{"load", "--quiet"}, mutates: true},
		verbRemove:        {prefix: []string{"rm", "-f", "-v"}, mutates: true},
		verbNetworkRemove: {prefix: []string{"network", "rm"}, mutates: true},
		verbVolumeRemove:  {prefix: []string{"volume", "rm"}, mutates: true},
		verbRemoveImage:   {prefix: []string{"rmi", "--no-prune"}, mutates: true},
	}
}

// Object is a kind of engine object a test reads back.
type Object string

// The kinds of object Inspect reads.
const (
	ObjectContainer Object = "container"
	ObjectImage     Object = "image"
	ObjectVolume    Object = "volume"
	ObjectNetwork   Object = "network"
	// objectTag is an extra image reference AddTag made; removing it untags that reference only.
	objectTag Object = "tag"
)

// inspectVerbs reads each kind of object.
func inspectVerbs() map[Object]verbID {
	return map[Object]verbID{
		ObjectContainer: verbInspect,
		ObjectImage:     verbImageInspect,
		ObjectVolume:    verbVolumeInspect,
		ObjectNetwork:   verbNetworkInspect,
	}
}

// removeVerbs removes each kind of object a helper made.
func removeVerbs() map[Object]verbID {
	return map[Object]verbID{
		ObjectContainer: verbRemove,
		ObjectImage:     verbRemoveImage,
		ObjectVolume:    verbVolumeRemove,
		ObjectNetwork:   verbNetworkRemove,
		objectTag:       verbRemoveImage,
	}
}

// CreateSpec is a container a test creates. Env reaches the container through stdin, never argv.
// Mounts are --mount values; Publish ports are published on 127.0.0.1 at a port the engine picks.
type CreateSpec struct {
	Env     map[string]string
	Labels  map[string]string
	Image   string
	Name    string
	Network string
	Cmd     []string
	Mounts  []string
	Publish []uint16
	// Unlabelled leaves the test label off, for a decoy whose point is carrying none. It is still
	// tracked and removed by its exact ID.
	Unlabelled bool
}

// Listing is what the engine lists under one label.
type Listing struct {
	Containers []string
	Networks   []string
	Volumes    []string
	Images     []string
}

var (
	// errNotOwned means a removal named something no helper in this test process created.
	errNotOwned = errors.New("no helper in this test process created it")
	// errRemoval means the engine refused to remove something a helper created.
	errRemoval = errors.New("removal failed")
	// errNotOneImage means a saved archive held other than exactly one image.
	errNotOneImage = errors.New("the archive does not hold exactly one image")
)

// owned is one resource a helper in this process created.
type owned struct {
	what    Object
	removed bool
}

var (
	//nolint:gochecknoglobals // one registry per test process: any helper may remove what another made.
	ownedMu sync.Mutex
	//nolint:gochecknoglobals // one registry per test process: any helper may remove what another made.
	ownedByID = map[string]*owned{}
	//nolint:gochecknoglobals // the docker CLI is resolved once per test process.
	dockerPath = sync.OnceValues(func() (string, error) { return exec.LookPath("docker") })
	//nolint:gochecknoglobals // one subnet sequence per test process, so its own networks never collide.
	subnetNext atomic.Uint32
	//nolint:gochecknoglobals // a fixed range, never written: where test networks take their subnets.
	testSubnets = netip.MustParsePrefix("10.232.0.0/16")
)

// Docker is the one route by which a test creates, reads and removes Docker resources. It runs the
// docker CLI from a closed verb table, in an environment of exactly PATH, DOCKER_HOST and an empty
// private DOCKER_CONFIG. It removes only what a helper in this test process created, by exact ID.
type Docker struct {
	engine Engine
	host   string
	config string
	calls  atomic.Int64
}

// Docker returns a helper bound to this engine.
func (e Engine) Docker(tb testing.TB) *Docker {
	tb.Helper()

	return &Docker{engine: e, host: e.Endpoint(), config: tb.TempDir()}
}

// Import makes an image from a layer tar, tagged ref when ref is not empty, with the test label and
// changes applied; it returns the image ID. A ref that already exists FAILS the test.
func (d *Docker) Import(tb testing.TB, layer io.Reader, ref string, changes []string) string {
	tb.Helper()

	if ref != "" {
		d.fresh(tb, ObjectImage, ref)
	}

	key, value := d.engine.TestLabel()
	args := []string{"--change", "LABEL " + key + "=" + value}

	for _, change := range changes {
		args = append(args, "--change", change)
	}

	args = append(args, "-")
	if ref != "" {
		args = append(args, ref)
	}

	id := d.must(tb, call{verb: verbImport, args: args, stdin: layer})
	d.own(tb, ObjectImage, id)

	return id
}

// Create creates a container, pulling its image first only when the engine does not hold it; it
// returns the container's full ID.
func (d *Docker) Create(tb testing.TB, spec CreateSpec) string {
	tb.Helper()

	if spec.Name != "" {
		d.fresh(tb, ObjectContainer, spec.Name)
	}

	image := d.Inspect(tb, ObjectImage, spec.Image)
	if image == nil {
		d.must(tb, call{verb: verbPull, args: []string{spec.Image}})
		image = d.Inspect(tb, ObjectImage, spec.Image)
	}

	labels := spec.Labels
	if key, _ := d.engine.TestLabel(); spec.Unlabelled {
		// An image's labels reach every container made from it, and a create only overrides a key.
		if _, carried := readLabelled(tb, image).Config.Labels[key]; carried {
			tb.Fatalf("an unlabelled container cannot come from %s: the image carries %s", spec.Image, key)
		}
	} else {
		labels = d.withTestLabel(labels)
	}

	args, stdin := envArgs(tb, spec.Env)
	args = append(args, labelArgs(labels)...)

	if spec.Name != "" {
		args = append(args, "--name", spec.Name)
	}

	if spec.Network != "" {
		args = append(args, "--network", spec.Network)
	}

	for _, mount := range spec.Mounts {
		args = append(args, "--mount", mount)
	}

	for _, port := range spec.Publish {
		args = append(args, "--publish", "127.0.0.1::"+strconv.Itoa(int(port)))
	}

	args = append(append(args, spec.Image), spec.Cmd...)

	id := d.must(tb, call{verb: verbCreate, args: args, stdin: stdin})
	d.own(tb, ObjectContainer, id)

	return id
}

// Start starts a container a helper in this process created.
func (d *Docker) Start(tb testing.TB, id string) {
	tb.Helper()

	if !isOwned(id) {
		tb.Fatalf("refusing to start %s: %v", id, errNotOwned)
	}

	d.must(tb, call{verb: verbStart, args: []string{id}})
}

// Kill stops a container with SIGKILL: one a helper in this process created, or one Stutter created
// (it carries Stutter's check label) and a developer's own database never is.
func (d *Docker) Kill(tb testing.TB, id string) {
	tb.Helper()

	if !isOwned(id) {
		raw := d.Inspect(tb, ObjectContainer, id)
		if raw == nil {
			tb.Fatalf("refusing to kill %s: the engine holds no such container", id)
		}

		read := readLabelled(tb, raw)
		if read.Config.Labels[rules.LabelCheck] == "" ||
			slices.Contains(reservedNames(), strings.TrimPrefix(read.Name, "/")) {
			tb.Fatalf("refusing to kill %s: neither this test process nor a Stutter check created it", id)
		}
	}

	d.must(tb, call{verb: verbKill, args: []string{id}})
}

// Wait waits for a container to exit and returns its exit code.
func (d *Docker) Wait(tb testing.TB, id string) int {
	tb.Helper()

	out := d.must(tb, call{verb: verbWait, args: []string{id}})

	code, err := strconv.Atoi(out)
	if err != nil {
		tb.Fatalf("wait %s printed %q, not an exit code", id, out)
	}

	return code
}

// Remove removes a resource a helper in this process created, by its exact ID. Any other ID FAILS
// the test before a call is made.
func (d *Docker) Remove(tb testing.TB, id string) {
	tb.Helper()

	if err := d.removeOwned(tb, id); err != nil {
		tb.Fatalf("%v", err)
	}
}

// CreateNetwork creates a network with the test label on subnet, and returns its full ID. A zero
// subnet takes the next /24 of a range kept for tests that no host interface uses, moving on while
// the engine reports the one tried overlaps a network it already has.
func (d *Docker) CreateNetwork(tb testing.TB, name string, subnet netip.Prefix, labels map[string]string) string {
	tb.Helper()

	if name == "" {
		tb.Fatal("a test network needs a name")
	}

	d.fresh(tb, ObjectNetwork, name)

	create := func(on netip.Prefix) answer {
		args := slices.Concat(labelArgs(d.withTestLabel(labels)), []string{"--subnet", on.String(), name})

		return d.run(tb, call{verb: verbNetworkCreate, args: args})
	}

	candidates := []netip.Prefix{subnet}
	if !subnet.IsValid() {
		candidates = make([]netip.Prefix, 0, subnetAttempts)
		for range subnetAttempts {
			candidates = append(candidates, nextTestSubnet(tb))
		}
	}

	var last answer

	for _, candidate := range candidates {
		last = create(candidate)
		if last.exit == 0 {
			id := strings.TrimSpace(string(last.out))
			d.own(tb, ObjectNetwork, id)

			return id
		}

		if subnet.IsValid() || !strings.Contains(last.stderr, "overlaps") {
			break
		}
	}

	tb.Fatalf("creating network %s on %v exited %d: %s", name, candidates, last.exit, last.stderr)

	return ""
}

// CreateVolume creates a volume with the test label and returns its name. An empty name lets the
// engine pick one.
func (d *Docker) CreateVolume(tb testing.TB, name string, labels map[string]string) string {
	tb.Helper()

	args := labelArgs(d.withTestLabel(labels))

	if name != "" {
		d.fresh(tb, ObjectVolume, name)
		args = append(args, name)
	}

	id := d.must(tb, call{verb: verbVolumeCreate, args: args})
	d.own(tb, ObjectVolume, id)

	return id
}

// CopyIn extracts a tar archive into dir inside a container a helper in this process created.
func (d *Docker) CopyIn(tb testing.TB, id, dir string, archive io.Reader) {
	tb.Helper()

	if !isOwned(id) {
		tb.Fatalf("refusing to copy into %s: %v", id, errNotOwned)
	}

	d.must(tb, call{verb: verbCopyIn, args: []string{id + ":" + dir}, stdin: archive})
}

// Inspect reads one object back from the engine, or returns nil when the engine holds no such
// object.
func (d *Docker) Inspect(tb testing.TB, what Object, id string) json.RawMessage {
	tb.Helper()

	v, ok := inspectVerbs()[what]
	if !ok {
		tb.Fatalf("no inspect for %q", what)
	}

	a := d.run(tb, call{verb: v, args: []string{id}})

	if a.exit != 0 {
		absent := strings.TrimSpace(string(a.out)) == "[]" &&
			(strings.Contains(strings.ToLower(a.stderr), "no such") || strings.Contains(a.stderr, "not found"))
		if absent {
			return nil
		}

		tb.Fatalf("inspecting %s %s exited %d: %s", what, id, a.exit, a.stderr)
	}

	var all []json.RawMessage
	if err := json.Unmarshal(a.out, &all); err != nil || len(all) != 1 {
		tb.Fatalf("inspecting %s %s: want one object, got %d (%v)", what, id, len(all), err)
	}

	return all[0]
}

// ImageTags maps every tagged image reference the engine holds to its image ID.
func (d *Docker) ImageTags(tb testing.TB) map[string]string {
	tb.Helper()

	tags := map[string]string{}

	for line := range strings.Lines(d.must(tb, call{verb: verbImages})) {
		ref, id, ok := strings.Cut(strings.TrimSpace(line), "\t")
		if ok && !strings.Contains(ref, "<none>") {
			tags[ref] = id
		}
	}

	return tags
}

// Listing lists every container, network, volume and image carrying label, given as a key or as
// key=value.
func (d *Docker) Listing(tb testing.TB, label string) Listing {
	tb.Helper()

	list := func(v verbID) []string {
		return strings.Fields(d.must(tb, call{verb: v, args: []string{"label=" + label}}))
	}

	return Listing{
		Containers: list(verbPs),
		Networks:   list(verbNetworkList),
		Volumes:    list(verbVolumeList),
		Images:     list(verbImagesLabelled),
	}
}

// Port returns the loopback address the engine published a container's TCP port on.
func (d *Docker) Port(tb testing.TB, id string, port uint16) netip.AddrPort {
	tb.Helper()

	out := d.must(tb, call{verb: verbPort, args: []string{id, strconv.Itoa(int(port)) + "/tcp"}})
	first, _, _ := strings.Cut(out, "\n")

	addr, err := netip.ParseAddrPort(strings.TrimSpace(first))
	if err != nil {
		tb.Fatalf("port %s %d printed %q: %v", id, port, out, err)
	}

	return addr
}

// AddTag gives image a second reference without making a new image: it saves the image, names the
// archive's one manifest ref, and loads the archive back, so both references resolve to one image
// ID. ref must name a registry domain, and must not exist yet. The new reference is untagged, by
// that exact reference, when tb ends.
func (d *Docker) AddTag(tb testing.TB, image, ref string) {
	tb.Helper()

	if domain, _, ok := strings.Cut(ref, "/"); !ok || !strings.ContainsAny(domain, ".:") {
		tb.Fatalf("extra tag %q must start with a registry domain", ref)
	}

	d.fresh(tb, ObjectImage, ref)

	var saved, named bytes.Buffer

	d.must(tb, call{verb: verbSave, args: []string{image}, stdout: &saved})

	if err := nameArchive(&saved, &named, ref); err != nil {
		tb.Fatalf("naming the saved archive of %s: %v", image, err)
	}

	d.must(tb, call{verb: verbLoad, stdin: &named})
	d.own(tb, objectTag, ref)
}

// Plugin returns the path and version of the CLI plugin named name, as the engine's info reports it
// under dockerConfig.
//
//nolint:nonamedreturns // two strings of one type: the names say which is which.
func (d *Docker) Plugin(tb testing.TB, dockerConfig, name string) (path, version string) {
	tb.Helper()

	out := d.must(tb, call{verb: verbInfo, config: dockerConfig})

	for line := range strings.Lines(out) {
		fields := strings.Split(strings.TrimRight(line, "\n"), "\t")
		if len(fields) == 3 && fields[0] == name {
			return fields[1], fields[2]
		}
	}

	tb.Fatalf("no %q plugin under DOCKER_CONFIG=%s; the engine lists:\n%s", name, dockerConfig, out)

	return "", ""
}

// call is one docker invocation.
type call struct {
	stdin  io.Reader
	stdout io.Writer
	config string
	args   []string
	verb   verbID
}

// answer is what a call printed and how it exited.
type answer struct {
	stderr string
	out    []byte
	exit   int
}

// run makes one call from the verb table. It is the only place a test spawns anything.
func (d *Docker) run(tb testing.TB, c call) answer {
	tb.Helper()

	docker, err := dockerPath()
	if err != nil {
		tb.Fatalf("no docker CLI on PATH: %v", err)
	}

	config := d.config
	if c.config != "" {
		config = c.config
	}

	// Detached from the test's own cancellation: cleanups run after it, and must still remove.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(tb.Context()), callLimit)
	defer cancel()

	argv := slices.Concat(decoyVerbs()[c.verb].prefix, c.args)

	var out, stderr bytes.Buffer

	cmd := exec.CommandContext(ctx, docker, argv...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "DOCKER_HOST=" + d.host, "DOCKER_CONFIG=" + config}
	cmd.Stdin = c.stdin
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if c.stdout != nil {
		cmd.Stdout = c.stdout
	}

	d.calls.Add(1)

	err = cmd.Run()

	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		tb.Fatalf("docker %s: %v", strings.Join(argv, " "), err)
	}

	first, _, _ := strings.Cut(strings.TrimSpace(stderr.String()), "\n")

	return answer{out: out.Bytes(), stderr: first, exit: cmd.ProcessState.ExitCode()}
}

// must makes a call that has to succeed and returns its trimmed stdout.
func (d *Docker) must(tb testing.TB, c call) string {
	tb.Helper()

	a := d.run(tb, c)
	if a.exit != 0 {
		tb.Fatalf("docker %s exited %d: %s", strings.Join(slices.Concat(decoyVerbs()[c.verb].prefix, c.args), " "),
			a.exit, a.stderr)
	}

	return strings.TrimSpace(string(a.out))
}

// own records a resource this process created and removes it by exact ID when tb ends. Cleanups run
// last-in first-out, so a container goes before the image, volume or network it uses.
func (d *Docker) own(tb testing.TB, what Object, id string) {
	tb.Helper()

	ownedMu.Lock()
	ownedByID[id] = &owned{what: what}
	ownedMu.Unlock()

	tb.Cleanup(func() {
		if err := d.removeOwned(tb, id); err != nil {
			tb.Errorf("cleanup: %v", err)
		}
	})
}

// removeOwned removes id once; a resource already removed is not an error.
func (d *Docker) removeOwned(tb testing.TB, id string) error {
	tb.Helper()

	ownedMu.Lock()
	entry, ok := ownedByID[id]
	ownedMu.Unlock()

	if !ok {
		return fmt.Errorf("refusing to remove %s: %w", id, errNotOwned)
	}

	if entry.removed {
		return nil
	}

	a := d.run(tb, call{verb: removeVerbs()[entry.what], args: []string{id}})
	if a.exit != 0 {
		return fmt.Errorf("%w: %s %s exited %d: %s", errRemoval, entry.what, id, a.exit, a.stderr)
	}

	ownedMu.Lock()
	entry.removed = true
	ownedMu.Unlock()

	return nil
}

// withTestLabel returns labels plus the test label.
func (d *Docker) withTestLabel(labels map[string]string) map[string]string {
	all := maps.Clone(labels)
	if all == nil {
		all = map[string]string{}
	}

	key, value := d.engine.TestLabel()
	all[key] = value

	return all
}

// fresh FAILS the test when name is reserved or already names an object of kind what.
func (d *Docker) fresh(tb testing.TB, what Object, name string) {
	tb.Helper()

	if slices.Contains(reservedNames(), name) {
		tb.Fatalf("%q is never a decoy's name: it is the developer's own", name)
	}

	if d.Inspect(tb, what, name) != nil {
		tb.Fatalf("refusing to create %s %q: one by that name already exists", what, name)
	}
}

// isOwned reports whether a helper in this process created id and has not removed it.
func isOwned(id string) bool {
	ownedMu.Lock()
	defer ownedMu.Unlock()

	entry, ok := ownedByID[id]

	return ok && !entry.removed
}

// labelArgs returns --label arguments for labels, in key order.
func labelArgs(labels map[string]string) []string {
	args := make([]string, 0, flagAndValue*len(labels))
	for _, key := range slices.Sorted(maps.Keys(labels)) {
		args = append(args, "--label", key+"="+labels[key])
	}

	return args
}

// envArgs returns the --env-file argument and the stdin carrying env, so no value reaches argv.
func envArgs(tb testing.TB, env map[string]string) ([]string, io.Reader) {
	tb.Helper()

	if len(env) == 0 {
		return nil, nil
	}

	var file strings.Builder

	for _, key := range slices.Sorted(maps.Keys(env)) {
		value := env[key]
		if key == "" || strings.ContainsAny(key, "=\n\r") || strings.ContainsAny(value, "\n\r") {
			tb.Fatalf("environment entry %q cannot travel in an env file", key)
		}

		file.WriteString(key + "=" + value + "\n")
	}

	return []string{"--env-file", "/dev/stdin"}, strings.NewReader(file.String())
}

// reservedNames are never a decoy's name: the developer's own databases, and any host a database DSN
// in the environment names.
func reservedNames() []string {
	names := []string{"stutter-test-pg", "stutter-target-db"}

	for _, pair := range os.Environ() {
		_, value, _ := strings.Cut(pair, "=")

		u, err := url.Parse(value)
		if err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") && u.Hostname() != "" {
			names = append(names, u.Hostname())
		}
	}

	return names
}

// labelledConfig is the part of an inspect's Config that holds labels.
type labelledConfig struct {
	Labels map[string]string `json:"Labels"` //nolint:tagliatelle // the engine's own field name
}

// labelled is the name and labels an inspect reports. Containers and images keep their labels under
// Config; volumes and networks at the top.
type labelled struct {
	Labels map[string]string `json:"Labels"` //nolint:tagliatelle // the engine's own field name
	Config labelledConfig    `json:"Config"` //nolint:tagliatelle // the engine's own field name
	Name   string            `json:"Name"`   //nolint:tagliatelle // the engine's own field name
}

// readLabelled decodes an inspect's name and labels.
func readLabelled(tb testing.TB, raw json.RawMessage) labelled {
	tb.Helper()

	var read labelled
	if err := json.Unmarshal(raw, &read); err != nil {
		tb.Fatalf("decoding an inspect: %v", err)
	}

	return read
}

// nextTestSubnet returns this process's next /24 of the test range that no host interface uses. The
// sequence starts at a random offset, so two test processes rarely begin on the same subnet.
func nextTestSubnet(tb testing.TB) netip.Prefix {
	tb.Helper()

	start, err := strconv.ParseUint(labelValue()[:4], 16, 32)
	if err != nil {
		tb.Fatalf("the test label value %q is not hex: %v", labelValue(), err)
	}

	subnetNext.CompareAndSwap(0, uint32(start))

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		tb.Fatalf("reading the host's interface addresses: %v", err)
	}

	base := testSubnets.Addr().As4()

	for {
		var n [4]byte

		binary.BigEndian.PutUint32(n[:], subnetNext.Add(1))

		candidate := netip.PrefixFrom(netip.AddrFrom4([4]byte{base[0], base[1], n[3], 0}), testSubnetBits)
		if !usedByHost(candidate, addrs) {
			return candidate
		}
	}
}

// usedByHost reports whether candidate overlaps a host interface's network.
func usedByHost(candidate netip.Prefix, addrs []net.Addr) bool {
	for _, addr := range addrs {
		prefix, err := netip.ParsePrefix(addr.String())
		if err == nil && prefix.Masked().Overlaps(candidate) {
			return true
		}
	}

	return false
}

// nameArchive copies a saved image archive holding one image, naming that image ref in both the
// legacy manifest and the OCI index.
func nameArchive(src io.Reader, dst io.Writer, ref string) error {
	in, out := tar.NewReader(src), tar.NewWriter(dst)

	for {
		header, err := in.Next()
		if errors.Is(err, io.EOF) {
			if closeErr := out.Close(); closeErr != nil {
				return fmt.Errorf("closing the archive: %w", closeErr)
			}

			return nil
		}

		if err != nil {
			return fmt.Errorf("reading the archive: %w", err)
		}

		body, err := io.ReadAll(in)
		if err != nil {
			return fmt.Errorf("reading %s: %w", header.Name, err)
		}

		body, err = nameEntry(header.Name, body, ref)
		if err != nil {
			return fmt.Errorf("%s: %w", header.Name, err)
		}

		header.Size = int64(len(body))

		if err := out.WriteHeader(header); err != nil {
			return fmt.Errorf("writing %s: %w", header.Name, err)
		}

		if _, err := out.Write(body); err != nil {
			return fmt.Errorf("writing %s: %w", header.Name, err)
		}
	}
}

// nameEntry returns an archive entry's body with ref named in it, when the entry is one that names
// images.
func nameEntry(name string, body []byte, ref string) ([]byte, error) {
	switch name {
	case "manifest.json":
		return nameManifest(body, ref)
	case "index.json":
		return nameIndex(body, ref)
	default:
		return body, nil
	}
}

// nameManifest sets the one image's RepoTags to ref.
func nameManifest(body []byte, ref string) ([]byte, error) {
	var manifest []map[string]any
	if err := json.Unmarshal(body, &manifest); err != nil {
		return nil, fmt.Errorf("decoding: %w", err)
	}

	if len(manifest) != 1 {
		return nil, errNotOneImage
	}

	manifest[0]["RepoTags"] = []string{ref}

	named, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("encoding: %w", err)
	}

	return named, nil
}

// nameIndex annotates the one manifest the index lists with ref, the way the engine names an image
// it saved by reference.
func nameIndex(body []byte, ref string) ([]byte, error) {
	var index map[string]any
	if err := json.Unmarshal(body, &index); err != nil {
		return nil, fmt.Errorf("decoding: %w", err)
	}

	manifests, ok := index["manifests"].([]any)
	if !ok || len(manifests) != 1 {
		return nil, errNotOneImage
	}

	entry, ok := manifests[0].(map[string]any)
	if !ok {
		return nil, errNotOneImage
	}

	entry["annotations"] = map[string]string{
		"io.containerd.image.name":          ref,
		"org.opencontainers.image.ref.name": ref[strings.LastIndexByte(ref, ':')+1:],
	}

	named, err := json.Marshal(index)
	if err != nil {
		return nil, fmt.Errorf("encoding: %w", err)
	}

	return named, nil
}
