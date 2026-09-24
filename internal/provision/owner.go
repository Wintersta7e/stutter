package provision

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"net/netip"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// This file is the one owner of every call that changes the engine. Each create is preceded by a
// synced intent and verified by inspect after it; each removal re-verifies the recorded ID and this
// check's labels first. Nothing here is forced beyond `rm -f`, and nothing is ever pruned.

// Templates for the reads that identify a resource before it is used or removed.
const (
	containerIDTemplate = `{"id":{{json .Id}},"labels":{{json .Config.Labels}}}`
	// An image's inspect fails outright on a key its JSON lacks, and an unlabelled image has no
	// Labels key: index returns null instead.
	imageIDTemplate   = `{"id":{{json .Id}},"labels":{{json (index .Config "Labels")}}}`
	networkIDTemplate = `{"id":{{json .Id}},"labels":{{json .Labels}}}`
	volumeTemplate    = `{"name":{{json .Name}},"labels":{{json .Labels}}}`
	// presenceTemplate prints a constant. It cannot fail on an object the engine holds, so an inspect
	// through it exits zero exactly when its reference resolves.
	presenceTemplate = `present`
)

var (
	errMoved        = errors.New("the reference was moved to another image")
	errStillPresent = errors.New("the engine still holds it after its removal")
	errNotHandle    = errors.New("the handle is not one this check created")
	errBadName      = errors.New("not a name Stutter gives a resource")
	errHostPath     = errors.New("a host path is removed with the check-private directory, not by the engine")
)

// networkRole is what a network role may look like: it becomes part of an engine name.
var networkRole = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

// Handle is a resource the check created: a *Container, a *Network or a *Volume. Only this package
// makes one, so no caller can name a resource to remove.
type Handle interface {
	handle() (seq int, check string)
}

// Container is a container the check created.
type Container struct {
	// spec is what the container was created from, kept so a start refused for a taken host port can
	// create it again.
	spec *ContainerSpec
	// hostPorts maps each published container port to the host port selected for it.
	hostPorts map[uint16]uint16
	id        string
	name      string
	check     string
	kind      rules.Kind
	// copies are the copies made into the container before it started, in order: a container created
	// again gets each of them again.
	copies []copied
	seq    int
}

// copied is one CopyIn.
type copied struct {
	dir   string
	files []File
}

// ID returns the container's ID.
func (c *Container) ID() string { return c.id }

// Name returns the container's engine name.
func (c *Container) Name() string { return c.name }

// Kind returns what the container is to the check.
func (c *Container) Kind() rules.Kind { return c.kind }

func (c *Container) handle() (int, string) { return c.seq, c.check }

// Network is a network the check created.
type Network struct {
	id    string
	name  string
	check string
	seq   int
}

// ID returns the network's ID.
func (n *Network) ID() string { return n.id }

// Name returns the network's engine name.
func (n *Network) Name() string { return n.name }

func (n *Network) handle() (int, string) { return n.seq, n.check }

// Volume is a named volume the check created.
type Volume struct {
	name  string
	check string
	kind  rules.Kind
	seq   int
}

// Name returns the volume's name, which is its only identity.
func (v *Volume) Name() string { return v.name }

// Kind returns what the volume is to the check.
func (v *Volume) Kind() rules.Kind { return v.kind }

func (v *Volume) handle() (int, string) { return v.seq, v.check }

// identified is what the identifying templates print.
type identified struct {
	Labels map[string]string `json:"labels"`
	ID     string            `json:"id"`
	Name   string            `json:"name"`
}

// ours reports labels that contain this check's ID and the intended kind. Verification is
// "contains", never "equals": the engine and compose add labels of their own.
func ours(labels map[string]string, check string, kind rules.Kind) bool {
	return labels[rules.LabelCheck] == check && labels[rules.LabelKind] == string(kind)
}

// notOurs names what a resource lacks to be provably this check's.
func notOurs(typ ResourceType, ref string, kind rules.Kind, labels map[string]string, check string) error {
	missing := rules.LabelCheck + "=" + check
	if labels[rules.LabelCheck] == check {
		missing = rules.LabelKind + "=" + string(kind)
	}

	return fmt.Errorf("%w: %s %s (kind %s) lacks %s", ErrNotOurs, typ, ref, kind, missing)
}

// owns resolves a handle to this check's record of it.
func (e *Engine) owns(h Handle) error {
	seq, check := h.handle()
	if _, ok := e.book.record(seq); !ok || check != e.id {
		return errNotHandle
	}

	return nil
}

// read runs one identifying read, an inspect whose arguments are its template and one reference,
// into into. A failed read is a missing object only when the engine answers a version call and the
// same inspect through presenceTemplate does not find the reference either; otherwise it is an
// error. Stderr is never consulted.
func (e *Engine) read(ctx context.Context, req request, into any) (bool, error) {
	res, err := e.run.call(ctx, req)
	if err == nil {
		return true, decodeJSON(res.out, into)
	}

	// Only a call that ran and exited non-zero can mean a missing object.
	if !exitedNonZero(err) {
		return false, err
	}

	if reachErr := e.reachable(ctx); reachErr != nil {
		return false, fmt.Errorf("%w, and the engine does not answer: %w", err, reachErr)
	}

	// A template that fails on the object's JSON exits non-zero too. An inspect that found the
	// object, or one that never finished, leaves it unreadable, never absent.
	probe := req
	probe.args = slices.Concat([]arg{{val: presenceTemplate}}, req.args[1:])

	if _, probeErr := e.run.call(ctx, probe); !exitedNonZero(probeErr) {
		return false, errors.Join(fmt.Errorf("%w: %s is not proven absent, and its read failed: %w", ErrEngine,
			req.args[1].val, err), probeErr)
	}

	return false, nil
}

// exitedNonZero reports a call that ran and exited non-zero.
func exitedNonZero(err error) bool {
	callErr, ran := errors.AsType[*CallError](err)

	return ran && callErr.ExitCode() > 0
}

// reachable proves the engine answers.
func (e *Engine) reachable(ctx context.Context) error {
	_, err := e.run.call(ctx, request{verb: verbVersion, args: []arg{{val: versionTemplate}}})

	return err
}

// intend allocates the resource's seq, names it, and syncs its intent.
func (e *Engine) intend(
	typ ResourceType, kind rules.Kind, service string, name func(seq int) string,
) (record, error) {
	seq := e.book.led.next()
	rec := record{seq: seq, typ: typ, kind: kind, service: service, name: name(seq), state: opIntent}

	err := e.book.note(entry{Seq: seq, Op: opIntent, Type: typ, Kind: kind, Name: rec.name, Service: service})

	return rec, err
}

// created records the ID the engine returned.
func (e *Engine) created(rec *record, id string) error {
	rec.id, rec.state = id, opCreated

	return e.book.note(entry{Seq: rec.seq, Op: opCreated, Type: rec.typ, ID: id})
}

// CreateNetwork creates one network for role on subnet, which the caller picked. It is one attempt:
// a subnet that is not a masked IPv4 prefix or that intersects a local interface is refused before
// any call (ErrSubnetOverlap); one the engine refuses because a network now holds it is
// ErrSubnetTaken; a network whose read-back differs from the request is removed.
func (e *Engine) CreateNetwork(ctx context.Context, role string, internal bool, subnet netip.Prefix) (*Network, error) {
	if !networkRole.MatchString(role) {
		return nil, fmt.Errorf("network role %q: %w", role, errBadName)
	}

	locals, err := e.interfaces()
	if err != nil {
		return nil, err
	}

	if refused := admitSubnet(subnet, locals); refused != nil {
		return nil, refused
	}

	rec, err := e.intend(ResourceNetwork, rules.KindNetwork, "", func(int) string { return networkName(e.id, role) })
	if err != nil {
		return nil, err
	}

	args := networkCreateArgs(rec.name, labelSet(e.id, rules.KindNetwork, ""), internal, subnet)

	res, err := e.run.call(ctx, request{verb: verbNetworkCreate, args: args})
	if err != nil {
		return nil, e.networkRefused(ctx, rec, subnet, err)
	}

	if err := e.created(&rec, strings.TrimSpace(string(res.out))); err != nil {
		return nil, err
	}

	if err := e.verifyNetwork(ctx, rec, internal, subnet, locals); err != nil {
		return nil, err
	}

	return &Network{id: rec.id, name: rec.name, check: e.id, seq: rec.seq}, nil
}

// networkRefused resolves a failed create's intent, then decides from what the engine now holds
// whether the subnet was taken.
func (e *Engine) networkRefused(ctx context.Context, rec record, subnet netip.Prefix, cause error) error {
	resolved := e.resolveIntent(ctx, e.book, rec)

	networks, err := e.engineNetworks(ctx)
	if err != nil {
		return errors.Join(cause, err, resolved)
	}

	for _, network := range networks {
		for _, prefix := range network.ipv4Subnets() {
			if prefix.Overlaps(subnet) {
				return errors.Join(fmt.Errorf("%w: %s intersects %s of network %s", ErrSubnetTaken, subnet, prefix,
					network.Name), resolved)
			}
		}
	}

	return errors.Join(cause, resolved)
}

// verifyNetwork proves a created network is this check's and is what was asked for. One without
// this check's labels is never touched; one that differs otherwise is removed.
func (e *Engine) verifyNetwork(
	ctx context.Context, rec record, internal bool, subnet netip.Prefix, locals []localAddr,
) error {
	var report networkReport

	found, err := e.read(ctx, request{verb: verbNetworkInspect, args: []arg{{val: networkTemplate}, {val: rec.id}}},
		&report)
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("%w: network %s is gone right after its create", ErrEngine, rec.id)
	}

	if report.ID != rec.id || !ours(report.Labels, e.id, rules.KindNetwork) {
		return notOurs(ResourceNetwork, rec.id, rules.KindNetwork, report.Labels, e.id)
	}

	others, err := e.engineNetworks(ctx)
	if err == nil {
		err = checkReadBack(report, subnet, internal, locals, others)
	}

	if err != nil {
		return errors.Join(err, e.removeRecorded(ctx, e.book, rec))
	}

	return e.book.note(entry{Seq: rec.seq, Op: opVerified, Type: ResourceNetwork})
}

// CreateVolume creates one named volume of kind. The engine returns an existing volume of the same
// name without applying any label, so a volume whose read-back lacks this check's labels is never
// used and never removed.
func (e *Engine) CreateVolume(ctx context.Context, kind rules.Kind, service string) (*Volume, error) {
	if !slices.Contains(rules.Kinds(), kind) {
		return nil, fmt.Errorf("volume kind %q: %w", kind, errBadName)
	}

	rec, err := e.intend(ResourceVolume, kind, service, func(seq int) string { return volumeName(e.id, kind, seq) })
	if err != nil {
		return nil, err
	}

	res, err := e.run.call(ctx, request{verb: verbVolumeCreate, args: volumeCreateArgs(rec.name,
		labelSet(e.id, kind, service))})
	if err != nil {
		return nil, errors.Join(err, e.resolveIntent(ctx, e.book, rec))
	}

	if noteErr := e.created(&rec, strings.TrimSpace(string(res.out))); noteErr != nil {
		return nil, noteErr
	}

	var report identified

	found, err := e.read(ctx, request{verb: verbVolumeInspect, args: []arg{{val: volumeTemplate}, {val: rec.name}}},
		&report)
	if err != nil {
		return nil, err
	}

	if !found || rec.id != rec.name || report.Name != rec.name || !ours(report.Labels, e.id, kind) {
		return nil, notOurs(ResourceVolume, rec.name, kind, report.Labels, e.id)
	}

	if err := e.book.note(entry{Seq: rec.seq, Op: opVerified, Type: ResourceVolume}); err != nil {
		return nil, err
	}

	return &Volume{name: rec.name, check: e.id, kind: kind, seq: rec.seq}, nil
}

// Remove removes a resource the check created, after verifying it is still the one the ledger
// names.
func (e *Engine) Remove(ctx context.Context, h Handle) error {
	if err := e.owns(h); err != nil {
		return err
	}

	seq, _ := h.handle()
	rec, _ := e.book.record(seq)

	return e.removeRecorded(ctx, e.book, rec)
}

// removal is how one resource type is identified and removed.
type removal struct {
	template string
	inspect  verb
	remove   verb
	byName   bool
}

func removalOf(typ ResourceType) (removal, bool) {
	switch typ {
	case ResourceContainer:
		return removal{template: containerIDTemplate, inspect: verbInspect, remove: verbRemove}, true
	case ResourceNetwork:
		return removal{template: networkIDTemplate, inspect: verbNetworkInspect, remove: verbNetworkRemove}, true
	case ResourceVolume:
		return removal{template: volumeTemplate, inspect: verbVolumeInspect, remove: verbVolumeRemove, byName: true},
			true
	case ResourceImage:
		return removal{template: imageIDTemplate, inspect: verbImageInspect, remove: verbImageRemove, byName: true},
			true
	case ResourceHostPath, ResourceAnonymousVolume:
		return removal{}, false
	}

	return removal{}, false
}

// removeRecorded removes one resource b records, verifying it against b's check first, and appends
// the outcome to b. An intent without its create is resolved by name instead; a resource already
// removed, or recorded absent, is left to the sweep.
func (e *Engine) removeRecorded(ctx context.Context, b *book, rec record) error {
	if rec.state == opIntent {
		return e.resolveIntent(ctx, b, rec)
	}

	if rec.state == opRemoved || rec.state == opAbsent {
		return nil
	}

	how, ok := removalOf(rec.typ)
	if !ok {
		return errHostPath
	}

	ref := rec.id
	if how.byName {
		ref = rec.name
	}

	if err := e.removeOne(ctx, b, rec, how, ref); err != nil {
		return err
	}

	if rec.typ != ResourceContainer {
		return nil
	}

	if b == e.book {
		e.releaseHeldHostPorts(rec.seq)
	}

	var errs []error

	for _, volume := range b.children(rec.seq) {
		errs = append(errs, e.removeRecorded(ctx, b, volume))
	}

	return errors.Join(errs...)
}

// removeOne verifies, removes, and verifies gone.
func (e *Engine) removeOne(ctx context.Context, b *book, rec record, how removal, ref string) error {
	var report identified

	found, err := e.read(ctx, request{verb: how.inspect, args: []arg{{val: how.template}, {val: ref}}}, &report)
	if err != nil {
		return b.failed(rec, err)
	}

	if !found {
		return b.note(entry{Seq: rec.seq, Op: opRemoved, Type: rec.typ})
	}

	if mismatch := verifyRecorded(rec, report, b.check); mismatch != nil {
		return b.failed(rec, mismatch)
	}

	if _, refused := e.run.call(ctx, request{verb: how.remove, args: []arg{{val: ref}}}); refused != nil {
		return b.failed(rec, refused)
	}

	found, err = e.read(ctx, request{verb: how.inspect, args: []arg{{val: how.template}, {val: ref}}}, &report)

	switch {
	case err != nil:
		return b.failed(rec, err)
	case found && (report.ID == rec.id || rec.typ == ResourceVolume && report.Name == rec.name):
		return b.failed(rec, fmt.Errorf("%s %s: %w", rec.typ, ref, errStillPresent))
	default:
		return b.note(entry{Seq: rec.seq, Op: opRemoved, Type: rec.typ})
	}
}

// verifyRecorded is the pre-remove check: the object read is the one recorded, with b's labels.
func verifyRecorded(rec record, report identified, check string) error {
	switch {
	case rec.typ == ResourceImage && report.ID != rec.id:
		return fmt.Errorf("%w (%s, not %s)", errMoved, report.ID, rec.id)
	case rec.typ == ResourceVolume && report.Name != rec.name,
		rec.typ != ResourceVolume && rec.typ != ResourceImage && report.ID != rec.id:
		return fmt.Errorf("%w: %s %s is not the one recorded", ErrNotOurs, rec.typ, rec.name)
	case !ours(report.Labels, check, rec.kind):
		return notOurs(rec.typ, rec.name, rec.kind, report.Labels, check)
	default:
		return nil
	}
}

// BuildSpec is one build of every Stutter-started service with a `build:` key.
type BuildSpec struct {
	// Render writes the compose model the build reads, given each service's reserved reference and
	// its own label set.
	Render func(tags map[string]string, labels map[string]map[string]string) ([]byte, error)
	// Services maps each service built to its kind: target, job or dependency.
	Services map[string]rules.Kind
	// Dir is the compose project directory.
	Dir string
}

// buildKinds are the kinds a build may make.
func buildKinds() []rules.Kind {
	return []rules.Kind{rules.KindTarget, rules.KindJob, rules.KindDependency}
}

// pull lands ref in the user's environment, cancelled with the run. A pulled image is never ledgered
// and never removed: it is the user's.
func (e *Engine) pull(ctx context.Context, ref, platform string) error {
	req := request{verb: verbPull, tail: []arg{{val: ref}}}
	if platform != "" {
		req.args = []arg{{val: "--platform"}, {val: platform}}
	}

	_, err := e.run.call(ctx, req)

	return err
}

// Build builds every service in one compose invocation under this check's project name, each image
// under a reference the check reserved and labelled as the check's with the service's own kind. The
// model goes on stdin; the output goes to the check's logs. Every image is then inspected on the
// pinned engine: one absent or without this check's labels is a named build failure.
func (e *Engine) Build(ctx context.Context, spec BuildSpec) (map[string]compose.Image, error) {
	if e.book.containerCreated() {
		return nil, fmt.Errorf("%w: a build asked for after the first container", ErrImage)
	}

	services := slices.Sorted(maps.Keys(spec.Services))
	tags, labels := map[string]string{}, map[string]map[string]string{}
	recs := make([]record, 0, len(services))

	for _, service := range services {
		kind := spec.Services[service]
		if !slices.Contains(buildKinds(), kind) {
			return nil, fmt.Errorf("%w: service %s cannot be built as a %s", ErrImage, service, kind)
		}

		rec, err := e.intend(ResourceImage, kind, service, func(seq int) string { return imageRef(e.id, kind, seq) })
		if err != nil {
			return nil, err
		}

		recs = append(recs, rec)
		tags[service], labels[service] = rec.name, labelSet(e.id, kind, service)
	}

	if err := e.runBuild(ctx, spec, recs, tags, labels); err != nil {
		return nil, errors.Join(err, e.resolveAll(ctx, recs))
	}

	out := make(map[string]compose.Image, len(recs))

	for _, rec := range recs {
		image, err := e.verifyImage(ctx, rec, "")
		if err != nil {
			return nil, fmt.Errorf("%w: the build of service %s: %w", ErrImage, rec.service, err)
		}

		out[rec.service] = image
	}

	return out, nil
}

// runBuild renders the model and runs the one compose build, its output in the check's logs.
func (e *Engine) runBuild(
	ctx context.Context, spec BuildSpec, recs []record, tags map[string]string, labels map[string]map[string]string,
) error {
	if len(recs) == 0 {
		return nil
	}

	model, err := spec.Render(tags, labels)
	if err != nil {
		return fmt.Errorf("%w: render the build model: %w", ErrImage, err)
	}

	logPath := filepath.Join(e.private, logsDir, "build-"+strconv.Itoa(recs[0].seq)+".log")

	output, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fileMode)
	if err != nil {
		return fmt.Errorf("%w: create the build log: %w", ErrImage, err)
	}

	req := request{
		verb: verbComposeBuild, dir: spec.Dir, stdin: bytes.NewReader(model), stdout: output, stderr: output,
		args: []arg{{val: e.Project()}},
	}

	for _, rec := range recs {
		req.tail = append(req.tail, arg{val: rec.service})
	}

	_, err = e.run.call(ctx, req)
	if closeErr := output.Close(); err == nil && closeErr != nil {
		err = closeErr
	}

	if err != nil {
		return fmt.Errorf("%w: the build failed; its output is in %s: %w", ErrImage, logPath, err)
	}

	return nil
}

// resolveAll settles the intents of a failed build by reference: an image that landed anyway with
// this check's labels is removed.
func (e *Engine) resolveAll(ctx context.Context, recs []record) error {
	errs := make([]error, 0, len(recs))

	for _, rec := range recs {
		errs = append(errs, e.resolveIntent(ctx, e.book, rec))
	}

	return errors.Join(errs...)
}

// Import makes an image from a tar on stdin, labelled as this check's with changes applied, under a
// reference the check reserved. It needs no builder and leaves no build cache.
func (e *Engine) Import(
	ctx context.Context, kind rules.Kind, archive io.Reader, changes []string,
) (compose.Image, error) {
	rec, err := e.intend(ResourceImage, kind, "", func(seq int) string { return imageRef(e.id, kind, seq) })
	if err != nil {
		return compose.Image{}, err
	}

	args := labelChanges(labelSet(e.id, kind, ""))
	for _, change := range changes {
		args = append(args, arg{val: "--change"}, arg{val: change})
	}

	res, err := e.run.call(ctx, request{verb: verbImport, stdin: archive, args: args, tail: []arg{{val: rec.name}}})
	if err != nil {
		return compose.Image{}, errors.Join(fmt.Errorf("%w: import %s: %w", ErrImage, rec.name, err),
			e.resolveIntent(ctx, e.book, rec))
	}

	return e.verifyImage(ctx, rec, strings.TrimSpace(string(res.out)))
}

// Commit makes an image of a container's writable layer — never its volumes, tmpfs or binds —
// labelled as this check's with kind, under a reference the check reserved first.
func (e *Engine) Commit(ctx context.Context, c *Container, kind rules.Kind) (compose.Image, error) {
	if err := e.owns(c); err != nil {
		return compose.Image{}, err
	}

	from, _ := e.book.record(c.seq)

	rec, err := e.intend(ResourceImage, kind, from.service, func(seq int) string { return imageRef(e.id, kind, seq) })
	if err != nil {
		return compose.Image{}, err
	}

	args := append(labelChanges(labelSet(e.id, kind, from.service)), arg{val: c.id})

	res, err := e.run.call(ctx, request{verb: verbCommit, args: args, tail: []arg{{val: rec.name}}})
	if err != nil {
		return compose.Image{}, errors.Join(fmt.Errorf("%w: commit %s: %w", ErrImage, c.name, err),
			e.resolveIntent(ctx, e.book, rec))
	}

	return e.verifyImage(ctx, rec, strings.TrimSpace(string(res.out)))
}

// labelChanges renders labels as `--change 'LABEL key="value"'`, in key order.
func labelChanges(labels map[string]string) []arg {
	out := make([]arg, 0, len(labels)+len(labels))

	for _, key := range slices.Sorted(maps.Keys(labels)) {
		out = append(out, arg{val: "--change"}, arg{val: "LABEL " + key + "=" + strconv.Quote(labels[key])})
	}

	return out
}

// verifyImage records the ID an image create returned — or, for a build, the one its reference
// resolves to — and proves by inspect that the reference names it with this check's labels, then
// pins it.
func (e *Engine) verifyImage(ctx context.Context, rec record, id string) (compose.Image, error) {
	image, found, err := e.inspectImage(ctx, rec.name)
	if err != nil {
		return compose.Image{}, err
	}

	if !found {
		return compose.Image{}, fmt.Errorf("%w: %s is absent on the engine", ErrImage, rec.name)
	}

	if id == "" {
		id = image.ID
	}

	if err := e.created(&rec, id); err != nil {
		return compose.Image{}, err
	}

	if image.ID != id || !ours(image.Labels, e.id, rec.kind) {
		return compose.Image{}, notOurs(ResourceImage, rec.name, rec.kind, image.Labels, e.id)
	}

	if err := e.book.note(entry{Seq: rec.seq, Op: opVerified, Type: ResourceImage}); err != nil {
		return compose.Image{}, err
	}

	e.pin(rec.name, image)

	return image, nil
}

// NetworkAttach is one network a container joins at create, with the names it answers to there.
type NetworkAttach struct {
	// Network is one of the check's networks.
	Network *Network
	// Aliases are the container's names on that network.
	Aliases []string
}

// VolumeMount is one of the check's named volumes mounted into a container.
type VolumeMount struct {
	// Volume is the named volume.
	Volume *Volume
	// Target is the absolute path inside the container.
	Target string
	// NoCopy skips copying the image's content into an empty volume.
	NoCopy bool
	// ReadOnly mounts it read-only; the read-back must agree.
	ReadOnly bool
}

// Healthcheck is a container's healthcheck. A nil *Healthcheck disables the image's.
type Healthcheck struct {
	// Test is compose's test: ["CMD", argv…], ["CMD-SHELL", command] or ["NONE"].
	Test []string
	// Interval, Timeout, StartPeriod and StartInterval are its durations; zero keeps the engine's.
	Interval      time.Duration
	Timeout       time.Duration
	StartPeriod   time.Duration
	StartInterval time.Duration
	// Retries is its retry count; zero keeps the engine's.
	Retries int
	// Inherit keeps the image's own healthcheck, as a snapshot's.
	Inherit bool
}

// ContainerSpec is one container to create: what it runs, from the compose model, and where the
// check attaches it.
type ContainerSpec struct {
	// Healthcheck is the container's healthcheck; nil disables the image's.
	Healthcheck *Healthcheck
	// Kind is what the container is to the check.
	Kind rules.Kind
	// Service is the compose service it serves, if any.
	Service string
	// DNS is the resolver the container uses, when set.
	DNS netip.Addr
	// Networks are the check's networks it joins at create.
	Networks []NetworkAttach
	// Volumes are the check's named volumes it mounts.
	Volumes []VolumeMount
	// Publish are the container ports published on the host's loopback.
	Publish []uint16
	// Spec is the process, environment and mounts, from the compose model.
	Spec compose.Spec
	// NoNetwork runs a helper with no network at all.
	NoNetwork bool
}

// File is one regular file copied into a container.
type File struct {
	// Path is the file's path relative to the directory it is copied into.
	Path string
	// Data is the file's content.
	Data []byte
	// Mode is its permission bits.
	Mode fs.FileMode
	// UID and GID own it; zero is root.
	UID int
	GID int
}

// CreateContainer creates one container, never started: from the image this check pinned, named
// and labelled as this check's, `--restart no`, logged locally, on the check's networks and storage
// only, every image VOLUME covered by a labelled volume, its environment on stdin, each published port
// on a loopback host port selected for it. The read-back is verified; a container that carries this
// check's labels but fails any property is removed before the failure is returned.
func (e *Engine) CreateContainer(ctx context.Context, spec ContainerSpec) (*Container, error) {
	return e.createAvoiding(ctx, spec, nil)
}

// createAvoiding is CreateContainer, selecting no host port in avoid. Every create, a first one or
// one made again after a refused start, goes through it.
func (e *Engine) createAvoiding(ctx context.Context, spec ContainerSpec, avoid []uint16) (*Container, error) {
	image, err := e.gate(ctx, spec)
	if err != nil {
		return nil, err
	}

	if oneName(spec.Kind) {
		if heldErr := e.replaceHeld(ctx, containerName(e.id, spec.Kind, 0)); heldErr != nil {
			return nil, heldErr
		}
	}

	hostPorts, err := reserveHostPorts(ctx, spec.Publish, avoid)
	if err != nil {
		return nil, err
	}

	container, err := e.createOn(ctx, spec, image, hostPorts)
	if err != nil {
		releaseHostPorts(hostPorts)

		return nil, err
	}

	return container, nil
}

// createOn creates spec's container publishing on hostPorts, which are reserved for it: intent,
// create, verification, then the verified mark, each in the ledger.
func (e *Engine) createOn(
	ctx context.Context, spec ContainerSpec, image compose.Image, hostPorts map[uint16]uint16,
) (*Container, error) {
	rec, err := e.intend(ResourceContainer, spec.Kind, spec.Service, func(seq int) string {
		return containerName(e.id, spec.Kind, seq)
	})
	if err != nil {
		return nil, err
	}

	call := createArgs(createPlan{
		labels: labelSet(e.id, spec.Kind, spec.Service), name: rec.name, image: image.ID, spec: spec,
		covers: uncovered(image.Volumes, spec), hostPorts: hostPorts, volumeLabels: []string{
			rules.LabelCheck + "=" + e.id, rules.LabelKind + "=" + string(spec.Kind),
		},
	})

	res, err := e.run.call(ctx, request{
		verb: verbCreate, args: call.args, stdin: bytes.NewReader(call.stdin), extraEnv: call.extra,
	})
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create %s: %w", rec.name, err), e.resolveIntent(ctx, e.book, rec))
	}

	if err := e.created(&rec, strings.TrimSpace(string(res.out))); err != nil {
		return nil, err
	}

	if err := e.verifyCreated(ctx, rec, spec, hostPorts); err != nil {
		return nil, err
	}

	if err := e.book.note(entry{Seq: rec.seq, Op: opVerified, Type: ResourceContainer}); err != nil {
		return nil, err
	}

	e.holdHostPorts(rec.seq, hostPorts)

	return &Container{
		id: rec.id, name: rec.name, check: e.id, kind: spec.Kind, seq: rec.seq, spec: &spec, hostPorts: hostPorts,
	}, nil
}

// uncovered returns the image VOLUME paths no mount of the spec lands exactly on.
func uncovered(volumes []string, spec ContainerSpec) []string {
	covered := map[string]bool{}

	for _, m := range spec.Spec.Mounts {
		covered[path.Clean(m.Target)] = true
	}

	for _, v := range spec.Volumes {
		covered[path.Clean(v.Target)] = true
	}

	var out []string

	for _, volume := range volumes {
		if !covered[path.Clean(volume)] {
			out = append(out, volume)
		}
	}

	return out
}

// verifyCreated proves a created container is this check's and is what was asked for. One without
// this check's labels is never touched. Its anonymous volumes are ledgered against it first, so a
// container that fails any other property is removed with them.
func (e *Engine) verifyCreated(ctx context.Context, rec record, spec ContainerSpec, hostPorts map[uint16]uint16) error {
	var report containerReport

	found, err := e.read(ctx, request{verb: verbInspect, args: []arg{{val: containerTemplate}, {val: rec.id}}},
		&report)
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("%w: container %s is gone right after its create", ErrEngine, rec.id)
	}

	if report.ID != rec.id || !ours(report.Labels, e.id, rec.kind) {
		return notOurs(ResourceContainer, rec.id, rec.kind, report.Labels, e.id)
	}

	anonymous, err := e.ledgerAnonymous(ctx, rec, report, spec)
	if err == nil {
		err = verifyContainer(report, e.expect(spec, anonymous, hostPorts))
	}

	if err != nil {
		return errors.Join(fmt.Errorf("container %s: %w", rec.name, err), e.removeRecorded(ctx, e.book, rec))
	}

	return nil
}

// ledgerAnonymous records every volume of the container that is not one of the spec's named volumes
// against it, and returns those carrying this check's labels.
func (e *Engine) ledgerAnonymous(
	ctx context.Context, rec record, report containerReport, spec ContainerSpec,
) (map[string]bool, error) {
	named := map[string]bool{}
	for _, v := range spec.Volumes {
		named[v.Volume.name] = true
	}

	labelled := map[string]bool{}

	for _, m := range report.Mounts {
		if m.Type != mountVolume || named[m.Name] {
			continue
		}

		if err := e.book.note(entry{
			Seq: e.book.led.next(), Op: opCreated, Type: ResourceVolume, Kind: rec.kind, Name: m.Name, ID: m.Name,
			Parent: rec.seq,
		}); err != nil {
			return nil, err
		}

		var volume identified

		found, err := e.read(ctx, request{verb: verbVolumeInspect, args: []arg{{val: volumeTemplate}, {val: m.Name}}},
			&volume)
		if err != nil {
			return nil, err
		}

		labelled[m.Name] = found && ours(volume.Labels, e.id, rec.kind)
	}

	return labelled, nil
}

// expect is what verifyContainer holds a created container to.
func (e *Engine) expect(spec ContainerSpec, anonymous map[string]bool, hostPorts map[uint16]uint16) expectation {
	x := expectation{
		binds: map[string]bool{}, named: map[string]bool{}, anonymous: anonymous, networks: map[string]bool{},
		ports: map[string]string{}, noNetwork: spec.NoNetwork,
	}

	for port, host := range hostPorts {
		x.ports[strconv.Itoa(int(port))+"/tcp"] = strconv.Itoa(int(host))
	}

	for _, m := range spec.Spec.Mounts {
		if m.Kind == compose.MountBind {
			x.binds[filepath.Clean(m.Source)] = true
		}
	}

	for _, v := range spec.Volumes {
		x.named[v.Volume.name] = v.ReadOnly
	}

	for _, rec := range e.book.all() {
		if rec.typ == ResourceNetwork && rec.state == opVerified {
			x.networks[rec.name] = true
		}
	}

	return x
}

// CopyIn copies files into a container's filesystem under dir: a tar of regular files only, owned by
// root unless a file says otherwise. Nothing lands at or under a bind destination of the container,
// where it would overwrite the host.
func (e *Engine) CopyIn(ctx context.Context, c *Container, dir string, files []File) error {
	report, err := e.inspectContainer(ctx, c)
	if err != nil {
		return err
	}

	var binds []string

	for _, m := range report.Mounts {
		if m.Type == mountBind {
			binds = append(binds, path.Clean(m.Destination))
		}
	}

	if !path.IsAbs(dir) || under(path.Clean(dir), binds) {
		return fmt.Errorf("%w: copy into %s: at or under a bind", ErrRefused, dir)
	}

	archive, err := regularFiles(dir, files, binds)
	if err != nil {
		return err
	}

	req := request{verb: verbCopyIn, stdin: archive, args: []arg{{val: c.id + ":" + dir}}}
	if _, err := e.run.call(ctx, req); err != nil {
		return err
	}

	c.copies = append(c.copies, copied{dir: dir, files: slices.Clone(files)})

	return nil
}

// regularFiles writes files as a tar of regular-file entries, refusing a path that escapes dir or
// lands under a bind.
func regularFiles(dir string, files []File, binds []string) (io.Reader, error) {
	var out bytes.Buffer

	writer := tar.NewWriter(&out)

	for _, file := range files {
		name := path.Clean(file.Path)
		if !fs.ValidPath(name) || name == "." || under(path.Join(dir, name), binds) {
			return nil, fmt.Errorf("%w: file %s under %s", ErrRefused, file.Path, dir)
		}

		header := &tar.Header{
			Typeflag: tar.TypeReg, Name: name, Mode: int64(file.Mode.Perm()), Uid: file.UID, Gid: file.GID,
			Size: int64(len(file.Data)), ModTime: time.Unix(0, 0),
		}

		if err := writer.WriteHeader(header); err != nil {
			return nil, fmt.Errorf("tar %s: %w", name, err)
		}

		if _, err := writer.Write(file.Data); err != nil {
			return nil, fmt.Errorf("tar %s: %w", name, err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("tar: %w", err)
	}

	return &out, nil
}

// under reports a path at or beneath any of roots.
func under(p string, roots []string) bool {
	for _, root := range roots {
		if p == root || strings.HasPrefix(p, strings.TrimSuffix(root, "/")+"/") {
			return true
		}
	}

	return false
}

// CopyOut returns the tar stream of a path in a container.
func (e *Engine) CopyOut(ctx context.Context, c *Container, p string) (io.ReadCloser, error) {
	if err := e.owns(c); err != nil {
		return nil, err
	}

	res, err := e.run.call(ctx, request{verb: verbCopyOut, args: []arg{{val: c.id + ":" + p}}})
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewReader(res.out)), nil
}

// errNotSeed means a graceful stop was asked of a container that is not a seed: every other stop
// is SIGKILL, whose writes never enter a run as late effects.
var errNotSeed = errors.New("only a seed container stops gracefully")

// Start starts a container, then begins the wait whose end Exited reports. The wait begins only
// once the container runs: a wait on a created container returns at once, which would read as an
// exit.
//
// A start the engine refuses because a published host port is taken is not the container's failure:
// the refused container is removed, created again on other host ports through the whole create
// protocol, given every copy the first one was given, and started, with c becoming the new one. A
// published port is therefore read after Start, never before. Past startAttempts refusals the start
// fails with ErrPortTaken, naming every host port tried and what the engine said.
func (e *Engine) Start(ctx context.Context, c *Container) error {
	if err := e.owns(c); err != nil {
		return err
	}

	var (
		refusals []string
		refused  []uint16
	)

	for {
		_, err := e.run.call(ctx, request{verb: verbStart, args: []arg{{val: c.id}}})
		if err == nil {
			break
		}

		if len(c.hostPorts) == 0 || !portRefused(err) {
			return err
		}

		refusals = append(refusals, fmt.Sprintf("host port %s: %v", hostPortList(c.hostPorts), err))
		refused = slices.AppendSeq(refused, maps.Values(c.hostPorts))

		if len(refusals) == startAttempts {
			return fmt.Errorf("%w: container %s was refused %d times: %s", ErrPortTaken, c.name, len(refusals),
				strings.Join(refusals, "; "))
		}

		if err := e.recreate(ctx, c, refused); err != nil {
			return errors.Join(fmt.Errorf("%w: container %s: %s", ErrPortTaken, c.name, strings.Join(refusals, "; ")),
				err)
		}
	}

	e.watchExit(ctx, c)

	return nil
}

// recreate replaces a container whose start was refused for a taken host port. The refused one is
// removed through the ledger, with its anonymous volumes; the new one is created from the same spec
// through the same create, on none of the refused host ports, and every copy made into the first is
// made into it. c becomes the new container.
func (e *Engine) recreate(ctx context.Context, c *Container, refused []uint16) error {
	rec, _ := e.book.record(c.seq)
	if err := e.removeRecorded(ctx, e.book, rec); err != nil {
		return err
	}

	fresh, err := e.createAvoiding(ctx, *c.spec, refused)
	if err != nil {
		return err
	}

	// c names the new container before the copies are made again, so a copy that fails leaves the
	// caller holding the container that exists.
	made := c.copies
	*c = *fresh

	for _, again := range made {
		if err := e.CopyIn(ctx, c, again.dir, again.files); err != nil {
			return err
		}
	}

	return nil
}

// Stop reads a container's state, kills it, and copies its log: a target's, a probe's, a
// discovery's and a job's always, and any container's whose state shows it failed. The state is
// read before the kill — afterwards every exit code is the kill's own.
func (e *Engine) Stop(ctx context.Context, c *Container) (State, error) {
	report, err := e.inspectContainer(ctx, c)
	if err != nil {
		return State{}, err
	}

	state := State{
		ExitCode: report.ExitCode, RestartCount: report.RestartCount, Running: report.Running,
		OOMKilled: report.OOMKilled,
	}

	rec, _ := e.book.record(c.seq)
	if killErr := e.killContainer(ctx, e.book, rec); killErr != nil {
		return state, killErr
	}

	if logWanted(c.kind, state) {
		state.Log, err = e.CopyLog(ctx, c)
	}

	return state, err
}

// logWanted reports a container whose log outlives it.
func logWanted(kind rules.Kind, s State) bool {
	return failedRun(s) || slices.Contains([]rules.Kind{
		rules.KindTarget, rules.KindProbe, rules.KindDiscovery, rules.KindJob,
	}, kind)
}

// failedRun reports a container that exited non-zero, was killed for memory, or was restarted.
func failedRun(s State) bool {
	return !s.Running && s.ExitCode != 0 || s.OOMKilled || s.RestartCount > 0
}

// Graceful is how a seed is stopped: compose's stop_signal and stop_grace_period.
type Graceful struct {
	// Signal is the stop signal; empty is the image's.
	Signal string
	// Grace is how long the seed may take to stop before the engine kills it.
	Grace time.Duration
	// HasGrace reports that compose set a grace period; otherwise the engine's default applies.
	HasGrace bool
}

// engineGrace is the engine's own stop timeout, the grace a seed gets when compose sets none.
const engineGrace = 10 * time.Second

// StopGracefully stops a seed with its compose signal and grace period, so a database flushes what
// it seeded; every other container is refused before any call. A seed still running when the grace
// period ends is killed by the engine, and the state says so.
func (e *Engine) StopGracefully(ctx context.Context, c *Container, g Graceful) (State, error) {
	if err := e.owns(c); err != nil {
		return State{}, err
	}

	if c.kind != rules.KindSeed {
		return State{}, fmt.Errorf("%w: %s is a %s", errNotSeed, c.name, c.kind)
	}

	req := request{verb: verbStopGraceful, grace: engineGrace}
	if g.Signal != "" {
		req.args = append(req.args, arg{val: "--signal"}, arg{val: g.Signal})
	}

	if g.HasGrace {
		req.grace = g.Grace
		req.args = append(req.args, arg{val: "--timeout"}, arg{val: strconv.Itoa(int(math.Ceil(g.Grace.Seconds())))})
	}

	req.args = append(req.args, arg{val: c.id})

	if _, err := e.run.call(ctx, req); err != nil {
		return State{}, err
	}

	state, err := e.Status(ctx, c)
	state.Killed = state.ExitCode == killedExit || state.OOMKilled

	return state, err
}

// killedExit is the exit code of a process ended by SIGKILL.
const killedExit = 137

// Retire ends a container's part in the check: it is stopped, then removed — except, under Keep,
// the target, probe or discovery, which is held stopped until the next container of its name is
// created, so the check's final one survives for the user.
func (e *Engine) Retire(ctx context.Context, c *Container) (State, error) {
	state, err := e.Stop(ctx, c)
	if err != nil {
		return state, err
	}

	if e.keep && oneName(c.kind) {
		e.mu.Lock()
		e.held[c.name] = c.seq
		e.mu.Unlock()

		return state, nil
	}

	return state, e.Remove(ctx, c)
}

// oneName reports a kind whose container keeps one name for the whole check.
func oneName(kind rules.Kind) bool {
	return kind == rules.KindTarget || kind == rules.KindProbe || kind == rules.KindDiscovery
}

// replaceHeld removes the held container of name, verified gone, before a successor takes its name.
func (e *Engine) replaceHeld(ctx context.Context, name string) error {
	e.mu.Lock()
	seq, held := e.held[name]
	e.mu.Unlock()

	if !held {
		return nil
	}

	rec, _ := e.book.record(seq)
	if err := e.removeRecorded(ctx, e.book, rec); err != nil {
		return fmt.Errorf("remove the held %s before its successor: %w", name, err)
	}

	e.mu.Lock()
	delete(e.held, name)
	e.mu.Unlock()

	return nil
}

// killContainer stops a container b records with SIGKILL, after verifying it is still that one:
// `stop --signal KILL` returns once it has stopped, and never waits on a SIGTERM the process may
// ignore.
func (e *Engine) killContainer(ctx context.Context, b *book, rec record) error {
	var report identified

	found, err := e.read(ctx, request{verb: verbInspect, args: []arg{{val: containerIDTemplate}, {val: rec.id}}},
		&report)
	if err != nil || !found {
		return err
	}

	if mismatch := verifyRecorded(rec, report, b.check); mismatch != nil {
		return mismatch
	}

	_, err = e.run.call(ctx, request{verb: verbKill, args: []arg{{val: rec.id}}})

	return err
}

// failed records a removal that did not happen and returns why. Only the first stderr line of a
// refusal is recorded.
func (b *book) failed(rec record, cause error) error {
	detail := cause.Error()

	if callErr, ok := errors.AsType[*CallError](cause); ok {
		detail = callErr.Stderr
	}

	return errors.Join(fmt.Errorf("remove %s %s: %w", rec.typ, rec.name, cause),
		b.note(entry{Seq: rec.seq, Op: opRemoveFailed, Type: rec.typ, Error: detail}))
}

// resolveIntent settles an intent whose create never recorded an ID: the daemon can finish a create
// after its client died. Found under its name with b's labels, and not an ID b already holds, it is
// treated as created and removed. Found without them, it is not b's: never touched, reported. Not
// found, it is absent.
func (e *Engine) resolveIntent(ctx context.Context, b *book, rec record) error {
	how, ok := removalOf(rec.typ)
	if !ok {
		return errHostPath
	}

	var report identified

	found, err := e.read(ctx, request{verb: how.inspect, args: []arg{{val: how.template}, {val: rec.name}}}, &report)
	if err != nil {
		return err
	}

	id := report.ID
	if rec.typ == ResourceVolume {
		id = report.Name
	}

	if !found || b.knows(id) {
		return b.note(entry{Seq: rec.seq, Op: opAbsent, Type: rec.typ})
	}

	if !ours(report.Labels, b.check, rec.kind) {
		return errors.Join(notOurs(rec.typ, rec.name, rec.kind, report.Labels, b.check),
			b.note(entry{Seq: rec.seq, Op: opAbsent, Type: rec.typ}))
	}

	rec.id, rec.state = id, opCreated

	if err := b.note(entry{Seq: rec.seq, Op: opCreated, Type: rec.typ, ID: id}); err != nil {
		return err
	}

	return e.removeRecorded(ctx, b, rec)
}
