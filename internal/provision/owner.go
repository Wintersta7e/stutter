package provision

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"slices"
	"strings"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// This file is the one owner of every call that changes the engine. Each create is preceded by a
// synced intent and verified by inspect after it; each removal re-verifies the recorded ID and this
// check's labels first. Nothing here is forced beyond `rm -f`, and nothing is ever pruned.

// Templates for the reads that identify a resource before it is used or removed.
const (
	containerIDTemplate = `{"id":{{json .Id}},"labels":{{json .Config.Labels}}}`
	imageIDTemplate     = `{"id":{{json .Id}},"labels":{{json .Config.Labels}}}`
	networkIDTemplate   = `{"id":{{json .Id}},"labels":{{json .Labels}}}`
	volumeTemplate      = `{"name":{{json .Name}},"labels":{{json .Labels}}}`
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
	id    string
	name  string
	check string
	kind  rules.Kind
	seq   int
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

// read runs one identifying read into into. A failed read on an engine that answers a version call
// is a missing object; on one that does not, it is an error. Stderr is never consulted.
func (e *Engine) read(ctx context.Context, req request, into any) (bool, error) {
	res, err := e.run.call(ctx, req)
	if err == nil {
		return true, decodeJSON(res.out, into)
	}

	// Only a call that ran and exited non-zero can mean a missing object.
	if callErr, ran := errors.AsType[*CallError](err); !ran || callErr.ExitCode() <= 0 {
		return false, err
	}

	if reachErr := e.reachable(ctx); reachErr != nil {
		return false, fmt.Errorf("%w, and the engine does not answer: %w", err, reachErr)
	}

	return false, nil
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
