package provision

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// teardownBound keeps a hung engine from holding the terminal: a toy check's teardown takes a few
// seconds.
const teardownBound = 60 * time.Second

// Retention is what the check-private directory keeps at Close.
type Retention uint8

const (
	// DiscardLogs removes the whole directory: the check reached a verdict and no rendered outcome
	// names a log.
	DiscardLogs Retention = iota
	// KeepLogs keeps the invocation log and the logs directory: an outcome names a log, the check was
	// interrupted, or it ended in a gate violation or a setup error.
	KeepLogs
)

// Counts are what a teardown did with one resource type.
type Counts struct {
	// Created is how many the check created.
	Created int
	// Removed is how many are verified gone.
	Removed int
	// Kept is how many were kept on purpose.
	Kept int
	// Failed is how many could not be removed.
	Failed int
}

// Retained is one kept or leftover resource that holds something secret.
type Retained struct {
	// Type is what the resource is.
	Type ResourceType
	// ID is its engine ID.
	ID string
	// Name is its engine name or image reference.
	Name string
	// Holds says what secret it holds.
	Holds string
	// Kind is what it is to the check.
	Kind rules.Kind
}

// Teardown is what Close did. Its text, WriteTo, is the check's closing audit.
type Teardown struct {
	// Err joins every failure; the verdict is never changed by one.
	Err error
	// Counts are per resource type; anonymous volumes are counted apart.
	Counts map[ResourceType]Counts
	// PrivateDir is the check-private directory, or "" once it is removed.
	PrivateDir string
	// Listing is everything the engine still holds carrying this check's label: empty unless the
	// check was kept, and anything else found is a defect.
	Listing []Listed
	// Leftovers are the check's resources a failure left behind.
	Leftovers []Listed
	// Retained names each kept or leftover resource that holds a secret, and what.
	Retained []Retained
	// Logs are the absolute paths of the log files kept.
	Logs []string
}

// Close tears the check down, once: under Keep every container is stopped and everything is kept;
// otherwise every resource the ledger records is removed — containers, networks, volumes, images —
// and the private directory is reduced by r. It runs under its own bound whatever ctx says.
func (e *Engine) Close(ctx context.Context, r Retention) Teardown {
	e.closeOnce.Do(func() {
		bound := e.teardownBound
		if bound <= 0 {
			bound = teardownBound
		}

		bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), bound)
		defer cancel()

		if e.keep {
			e.down = e.keepAll(bounded)
		} else {
			e.down = e.removeAll(bounded, r)
		}
	})

	return e.down
}

// keepAll stops every container and keeps everything, naming what holds a secret.
func (e *Engine) keepAll(ctx context.Context) Teardown {
	var errs []error

	for _, rec := range e.book.newestFirst(ResourceContainer) {
		if rec.id != "" && rec.state != opRemoved {
			errs = append(errs, e.killContainer(ctx, e.book, rec))
		}
	}

	listing, err := e.audit(ctx)
	errs = append(errs, err, e.book.noteOnce(opKept), e.log.close())

	down := Teardown{Listing: listing, Logs: logFiles(e.private), PrivateDir: e.private}
	down.Counts, down.Retained = e.tally(), e.secrets()
	down.Err = errors.Join(errs...)

	return down
}

// removeAll removes every resource the ledger records, audits what is left, then settles the
// private directory and the ledger. The private directory goes last: nothing is logged after it.
func (e *Engine) removeAll(ctx context.Context, r Retention) Teardown {
	errs := []error{e.removeEverything(ctx)}

	listing, err := e.audit(ctx)
	errs = append(errs, err, e.log.close())

	down := Teardown{Listing: listing}

	if r == KeepLogs {
		errs = append(errs, reduceToLogs(e.private), e.book.noteOnce(opRetained))
		down.Logs, down.PrivateDir = logFiles(e.private), e.private
	} else {
		errs = append(errs, removeMade(e.private), e.book.hostPathGone(e.private))
	}

	down.Counts, down.Leftovers, down.Retained = e.tally(), e.leftovers(), e.secrets()

	if len(down.Leftovers) == 0 && r == DiscardLogs && e.book.allRemoved() {
		errs = append(errs, e.book.led.remove())
	}

	down.Err = errors.Join(errs...)

	return down
}

// removeEverything settles interrupted intents, then removes containers, networks, volumes and
// images, each type newest first. A container's anonymous volumes go with it.
func (e *Engine) removeEverything(ctx context.Context) error {
	var errs []error

	for _, typ := range []ResourceType{ResourceContainer, ResourceNetwork, ResourceVolume, ResourceImage} {
		for _, rec := range e.book.newestFirst(typ) {
			errs = append(errs, e.removeRecorded(ctx, e.book, rec))
		}
	}

	return errors.Join(errs...)
}

// audit lists everything the engine holds with this check's exact label, and every anonymous
// volume the ledger holds against a container that the engine still has.
func (e *Engine) audit(ctx context.Context) ([]Listed, error) {
	listing, err := e.listLabelled(ctx, rules.LabelCheck+"="+e.id)
	errs := []error{err}

	for _, rec := range e.book.all() {
		if rec.typ != ResourceVolume || rec.parent == 0 || listedID(listing, rec.name) {
			continue
		}

		var report identified

		found, err := e.read(ctx, request{verb: verbVolumeInspect, args: []arg{{val: volumeTemplate}, {val: rec.name}}},
			&report)
		errs = append(errs, err)

		if found {
			listing = append(listing, Listed{
				Type: ResourceAnonymousVolume, ID: rec.name, Name: rec.name, Check: e.id, Kind: rec.kind,
			})
		}
	}

	return listing, errors.Join(errs...)
}

func listedID(listing []Listed, id string) bool {
	for _, l := range listing {
		if l.ID == id {
			return true
		}
	}

	return false
}

// typeOfRecord is a record's type as counted and listed: an anonymous volume apart.
func typeOfRecord(rec record) ResourceType {
	if rec.typ == ResourceVolume && rec.parent != 0 {
		return ResourceAnonymousVolume
	}

	return rec.typ
}

// tally counts every record by type.
func (e *Engine) tally() map[ResourceType]Counts {
	out := map[ResourceType]Counts{}

	for _, rec := range e.book.all() {
		if rec.state == opIntent || rec.state == opAbsent {
			continue
		}

		typ := typeOfRecord(rec)
		counts := out[typ]
		counts.Created++

		switch {
		case rec.state == opRemoved:
			counts.Removed++
		case e.keep || rec.typ == ResourceHostPath:
			counts.Kept++
		default:
			counts.Failed++
		}

		out[typ] = counts
	}

	return out
}

// leftovers lists the engine resources a failed teardown left.
func (e *Engine) leftovers() []Listed {
	var out []Listed

	for _, rec := range e.book.all() {
		if rec.typ != ResourceHostPath && rec.id != "" && rec.state != opRemoved {
			listed := listedOf(rec, e.id)
			listed.Type = typeOfRecord(rec)
			out = append(out, listed)
		}
	}

	return out
}

// secrets names every resource the check still holds that holds a secret: everything under Keep,
// the leftovers otherwise.
func (e *Engine) secrets() []Retained {
	var out []Retained

	for _, rec := range e.book.all() {
		holds := holdsSecret(rec)
		if holds == "" || rec.id == "" || rec.state == opRemoved {
			continue
		}

		out = append(out, Retained{Type: typeOfRecord(rec), ID: rec.id, Name: rec.name, Holds: holds, Kind: rec.kind})
	}

	return out
}

// holdsSecret says what a resource holds that is secret, or "": a snapshot holds the compose
// environment and the seed data, a container made from a compose service its environment, and a
// template or restore volume the seed data.
func holdsSecret(rec record) string {
	switch {
	case rec.typ == ResourceImage && rec.kind == rules.KindSnapshot:
		return "the compose environment and seed data"
	case rec.typ == ResourceContainer && composeDerived(rec):
		return "the compose environment"
	case rec.typ == ResourceVolume && rec.parent == 0 &&
		(rec.kind == rules.KindTemplateVolume || rec.kind == rules.KindRestore):
		return "seed data"
	default:
		return ""
	}
}

// composeDerived reports a container made from a compose service definition.
func composeDerived(rec record) bool {
	derived := []rules.Kind{
		rules.KindTarget, rules.KindProbe, rules.KindDiscovery, rules.KindJob, rules.KindSeed, rules.KindRestore,
	}

	return slices.Contains(derived, rec.kind) || rec.kind == rules.KindVerifier && rec.service != ""
}

// logFiles lists the log files a private directory keeps, as absolute paths.
func logFiles(private string) []string {
	var out []string

	if info, err := os.Lstat(filepath.Join(private, invocationLog)); err == nil && info.Mode().IsRegular() {
		out = append(out, filepath.Join(private, invocationLog))
	}

	logs, err := os.ReadDir(filepath.Join(private, logsDir))
	if err != nil {
		return out
	}

	for _, item := range logs {
		if item.Type().IsRegular() {
			out = append(out, filepath.Join(private, logsDir, item.Name()))
		}
	}

	return out
}

// WriteTo writes the teardown's audit: one count line per resource type, anonymous volumes apart;
// every resource the engine still holds with the check's label; each retained resource and what
// it holds; and every leftover.
func (t Teardown) WriteTo(w io.Writer) (int64, error) {
	var text strings.Builder

	for _, typ := range []ResourceType{
		ResourceContainer, ResourceNetwork, ResourceVolume, ResourceAnonymousVolume, ResourceImage, ResourceHostPath,
	} {
		c := t.Counts[typ]
		fmt.Fprintf(&text, "teardown %s created=%d removed=%d kept=%d failed=%d\n", typ, c.Created, c.Removed,
			c.Kept, c.Failed)
	}

	for _, l := range t.Listing {
		fmt.Fprintf(&text, "listed %s %s %s check=%s kind=%s\n", l.Type, l.ID, l.Name, l.Check, l.Kind)
	}

	for _, r := range t.Retained {
		fmt.Fprintf(&text, "retained %s %s %s holds %s\n", r.Type, r.ID, r.Name, r.Holds)
	}

	for _, l := range t.Leftovers {
		fmt.Fprintf(&text, "leftover %s %s %s\n", l.Type, l.ID, l.Name)
	}

	n, err := io.WriteString(w, text.String())

	return int64(n), err
}
