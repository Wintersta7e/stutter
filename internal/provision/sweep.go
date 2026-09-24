package provision

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// ResourceAnonymousVolume is an anonymous volume in a listing: counted apart from named volumes.
const ResourceAnonymousVolume ResourceType = "anonymous-volume"

// errNotItsRoot means a ledger's recorded private directory is not a path Stutter would have made
// for that check.
var errNotItsRoot = errors.New("not the check-private directory of that check")

// Listed is one resource named in a sweep, a teardown or a clean.
type Listed struct {
	// Type is what the resource is.
	Type ResourceType
	// ID is its engine ID; a volume's is its name.
	ID string
	// Name is its engine name, image reference or host path.
	Name string
	// Check is the check ID its labels or ledger name.
	Check string
	// Kind is what it is to that check.
	Kind rules.Kind
}

// SweepResult is what the sweep at Open found and did. A sweep failure never fails Open.
type SweepResult struct {
	// Unledgered lists, by check ID, resources carrying the check label that no ledger here names:
	// listed, never removed.
	Unledgered map[string][]Listed
	// Swept are the check IDs of the dead ledgers the sweep acted on.
	Swept []string
	// Skipped are the check IDs of ledgers not provably dead, or kept.
	Skipped []string
	// Corrupt are the check IDs of ledgers that could not be read whole: never acted on.
	Corrupt []string
	// Failed are resources a swept ledger names that could not be verified or removed.
	Failed []Listed
	// TmpDeleted counts leftover temporary ledgers deleted.
	TmpDeleted int
	// Disabled reports that locks are not enforced here, so nothing was removed.
	Disabled bool
}

// Sweep returns what the sweep at Open found and did.
func (e *Engine) Sweep() SweepResult {
	return e.swept
}

// sweep removes what every provably dead check on this engine and host left, and lists labelled
// resources no ledger here names. There is no age rule: a live check can run for hours.
func (e *Engine) sweepAll(ctx context.Context) SweepResult {
	result := SweepResult{Unledgered: map[string][]Listed{}}
	dir := filepath.Dir(e.book.led.path)

	ledgers, err := filesEnding(dir, ".ledger")
	if err != nil {
		result.Failed = append(result.Failed, Listed{Type: ResourceHostPath, Name: dir})
	}

	ledgered := map[string]bool{e.id: true}

	for _, path := range ledgers {
		ledgered[strings.TrimSuffix(filepath.Base(path), ".ledger")] = true
	}

	for _, path := range ledgers {
		switch {
		case path == e.book.led.path:
		case !e.locks:
			result.Skipped = append(result.Skipped, checkOf(path))
		default:
			e.sweepLedger(ctx, path, &result)
		}
	}

	if e.locks {
		result.TmpDeleted = e.deleteFreeTemps(dir)
	}

	result.Disabled = !e.locks
	e.listUnledgered(ctx, ledgered, &result)

	return result
}

// filesEnding lists the files of dir whose names end in suffix, sorted.
func filesEnding(dir, suffix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}

	var out []string

	for _, item := range entries {
		if strings.HasSuffix(item.Name(), suffix) {
			out = append(out, filepath.Join(dir, item.Name()))
		}
	}

	return out, nil
}

func checkOf(path string) string {
	return strings.TrimSuffix(strings.TrimSuffix(filepath.Base(path), ".tmp"), ".ledger")
}

// sweepLedger sweeps one other ledger if, and only if, its owner is provably dead.
func (e *Engine) sweepLedger(ctx context.Context, path string, result *SweepResult) {
	check := checkOf(path)

	first, err := loadLedger(path)
	if err != nil {
		result.Failed = append(result.Failed, Listed{Type: ResourceHostPath, Name: path, Check: check})

		return
	}

	if first.corrupt {
		result.Corrupt = append(result.Corrupt, check)

		return
	}

	self := e.book.led.header
	if first.header.EngineID != e.identity.EngineID || first.header.Host != self.Host {
		result.Skipped = append(result.Skipped, check)

		return
	}

	led, locked := e.lockLedger(path)
	if !locked {
		result.Skipped = append(result.Skipped, check)

		return
	}

	defer led.close()

	b, err := reloadBook(led)

	switch {
	case err != nil:
		result.Corrupt = append(result.Corrupt, check)
	case b.kept || !e.dead(b.led.header) || b.retained && b.engineDone(nil):
		result.Skipped = append(result.Skipped, check)
	default:
		result.Swept = append(result.Swept, check)
		e.sweepDead(ctx, b, result)
	}
}

// lockLedger opens another check's ledger for appending and takes its lock, held until close.
func (e *Engine) lockLedger(path string) (*ledger, bool) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return nil, false
	}

	locked, err := e.host.tryLock(file)
	if err != nil || !locked {
		_ = file.Close() // nothing was written; the descriptor only probed the lock

		return nil, false
	}

	return &ledger{file: file, host: e.host, path: path}, true
}

// reloadBook reads a locked ledger again and folds it: its owner cannot append once its lock is
// ours.
func reloadBook(led *ledger) (*book, error) {
	loaded, err := loadLedger(led.path)
	if err != nil {
		return nil, err
	}

	if loaded.corrupt {
		return nil, fmt.Errorf("ledger %s: %w", led.path, errCorrupt)
	}

	led.header = loaded.header
	b := newBook(led, loaded.header.Check)

	for _, en := range loaded.entries {
		b.fold(en)
	}

	return b, nil
}

// errCorrupt means a ledger has a malformed line other than its last.
var errCorrupt = errors.New("the ledger is corrupt")

// dead reports a ledger whose owner cannot be running: another boot, or — only within this PID
// namespace, where its PID means something — no process with its PID and start time.
func (e *Engine) dead(h header) bool {
	self := e.book.led.header
	if h.BootID != self.BootID {
		return true
	}

	if h.PIDNS != self.PIDNS {
		return false
	}

	start, err := e.host.startTime(h.PID)

	return err != nil || start != h.StartTime
}

// sweepDead removes a dead check's resources — containers, then networks, volumes and images, each
// type in reverse creation order, verified against its own check — then reduces its private
// directory to its logs. The ledger goes only when nothing of it is left.
func (e *Engine) sweepDead(ctx context.Context, b *book, result *SweepResult) {
	confirmed := map[int]bool{}

	for _, rec := range b.all() {
		if rec.state == opAbsent {
			confirmed[rec.seq] = true
		}
	}

	failed := e.removeEngineResources(ctx, b, result)

	logs, err := e.reducePrivate(b)
	if err != nil {
		failed = true

		result.Failed = append(result.Failed, Listed{
			Type: ResourceHostPath, Name: b.led.header.PrivateDir, Check: b.check,
		})
	}

	if failed || !b.engineDone(confirmed) {
		return
	}

	settled := b.led.remove
	if logs {
		settled = func() error { return b.noteOnce(opRetained) }
	}

	if err := settled(); err != nil {
		result.Failed = append(result.Failed, Listed{Type: ResourceHostPath, Name: b.led.path, Check: b.check})
	}
}

// removeEngineResources removes every engine resource b records — containers, then networks,
// volumes and images, each type newest first — and re-resolves each absent intent. It reports
// whether any was not removed.
func (e *Engine) removeEngineResources(ctx context.Context, b *book, result *SweepResult) bool {
	failed := false

	for _, typ := range []ResourceType{ResourceContainer, ResourceNetwork, ResourceVolume, ResourceImage} {
		for _, rec := range b.newestFirst(typ) {
			settle := e.removeRecorded
			if rec.state == opAbsent {
				settle = e.resolveIntent
			}

			if err := settle(ctx, b, rec); err != nil {
				failed = true

				result.Failed = append(result.Failed, listedOf(rec, b.check))
			}
		}
	}

	return failed
}

func listedOf(rec record, check string) Listed {
	return Listed{Type: rec.typ, ID: rec.id, Name: rec.name, Check: check, Kind: rec.kind}
}

// reducePrivate reduces a dead check's private directory to its log files, or removes it when none
// remain. The path comes from the ledger, so it is re-validated first: absolute, named for that
// check, and a private directory of this user — otherwise it is left untouched and reported.
func (e *Engine) reducePrivate(b *book) (bool, error) {
	path, exists, err := e.recordedPrivate(b)
	if err != nil {
		return false, err
	}

	if !exists {
		return false, b.hostPathGone(path)
	}

	if err := reduceToLogs(path); err != nil {
		return false, err
	}

	if hasLogs(path) {
		return true, nil
	}

	if err := removeMade(path); err != nil {
		return false, err
	}

	return false, b.hostPathGone(path)
}

// recordedPrivate re-validates the private directory another check's ledger records: absolute,
// named for that check, and — when it exists — a private directory of this user on a local
// filesystem.
func (e *Engine) recordedPrivate(b *book) (string, bool, error) {
	path := b.led.header.PrivateDir
	if !filepath.IsAbs(path) || filepath.Base(path) != "stutter-"+b.check {
		return path, false, fmt.Errorf("%w: %s", errNotItsRoot, path)
	}

	if _, err := e.host.lstat(path); errors.Is(err, fs.ErrNotExist) {
		return path, false, nil
	}

	if err := e.host.private(path); err != nil {
		return path, false, fmt.Errorf("%w: %s: %w", ErrPrivateDir, path, err)
	}

	return path, true, nil
}

// reduceToLogs removes every entry of a private directory but the invocation log and the logs
// directory. Nothing is followed: a symlinked entry is removed as a link.
func reduceToLogs(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	var errs []error

	for _, item := range entries {
		if item.Name() == invocationLog || item.Name() == logsDir {
			continue
		}

		errs = append(errs, removeMade(filepath.Join(path, item.Name())))
	}

	return errors.Join(errs...)
}

// hasLogs reports whether a private directory still holds a log file.
func hasLogs(path string) bool {
	if info, err := os.Lstat(filepath.Join(path, invocationLog)); err == nil && info.Mode().IsRegular() {
		return true
	}

	logs, err := os.ReadDir(filepath.Join(path, logsDir))

	return err == nil && len(logs) > 0
}

// deleteFreeTemps deletes every temporary ledger whose lock is free: the rename precedes every
// create, so it holds no intent.
func (e *Engine) deleteFreeTemps(dir string) int {
	temps, err := filesEnding(dir, ".ledger.tmp")
	if err != nil {
		return 0
	}

	deleted := 0

	for _, path := range temps {
		led, locked := e.lockLedger(path)
		if !locked {
			continue
		}

		if err := led.remove(); err == nil {
			deleted++
		}
	}

	return deleted
}

// listUnledgered lists every resource carrying the check label whose check has no ledger here.
func (e *Engine) listUnledgered(ctx context.Context, ledgered map[string]bool, result *SweepResult) {
	unledgered, err := e.unledgered(ctx, ledgered)
	if err != nil {
		result.Failed = append(result.Failed, Listed{Name: "listing: " + err.Error()})
	}

	result.Unledgered = unledgered
}

// unledgered groups, by check ID, every resource carrying the check label whose check has no
// ledger here.
func (e *Engine) unledgered(ctx context.Context, ledgered map[string]bool) (map[string][]Listed, error) {
	items, err := e.listLabelled(ctx, rules.LabelCheck)
	out := map[string][]Listed{}

	for _, item := range items {
		if !ledgered[item.Check] {
			out[item.Check] = append(out[item.Check], item)
		}
	}

	return out, err
}

// listing is how one resource type is listed by label.
type listing struct {
	template string
	typ      ResourceType
	verb     verb
}

// labelledTemplate prints a listed resource with its check and kind labels.
func labelledTemplate(id, name string) string {
	return `{"id":{{json .` + id + `}},"name":{{json .` + name + `}},` +
		`"check":{{json (.Label "` + rules.LabelCheck + `")}},"kind":{{json (.Label "` + rules.LabelKind + `")}}}`
}

// imageListTemplate prints a listed image; `images` has no label accessor, so each is inspected.
const imageListTemplate = `{"id":{{json .ID}},"name":{{json .Repository}},"tag":{{json .Tag}}}`

func listings() []listing {
	return []listing{
		{verb: verbContainerList, typ: ResourceContainer, template: labelledTemplate("ID", "Names")},
		{verb: verbNetworkList, typ: ResourceNetwork, template: labelledTemplate("ID", "Name")},
		{verb: verbVolumeList, typ: ResourceVolume, template: labelledTemplate("Name", "Name")},
		{verb: verbImageList, typ: ResourceImage, template: imageListTemplate},
	}
}

// listedReport is what the listing templates print.
type listedReport struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Tag   string `json:"tag"`
	Check string `json:"check"`
	Kind  string `json:"kind"`
}

// listLabelled lists every container, network, volume and image carrying the label selector
// (`key` or `key=value`). It only reads, and it is never an authority to remove.
func (e *Engine) listLabelled(ctx context.Context, selector string) ([]Listed, error) {
	var (
		out  []Listed
		errs []error
	)

	for _, l := range listings() {
		res, err := e.run.call(ctx, request{verb: l.verb, args: []arg{
			{val: l.template}, {val: "--filter"}, {val: "label=" + selector},
		}})
		if err != nil {
			errs = append(errs, err)

			continue
		}

		items, err := e.decodeListing(ctx, l.typ, res.out)
		errs = append(errs, err)
		out = append(out, items...)
	}

	return out, errors.Join(errs...)
}

// decodeListing reads one listing's lines. An image carries no label in its listing, so each image
// is inspected for its check and kind.
func (e *Engine) decodeListing(ctx context.Context, typ ResourceType, out []byte) ([]Listed, error) {
	var items []Listed

	for line := range strings.Lines(string(out)) {
		if strings.TrimSpace(line) == "" {
			continue
		}

		var report listedReport
		if err := decodeJSON([]byte(line), &report); err != nil {
			return items, err
		}

		item := Listed{Type: typ, ID: report.ID, Name: report.Name, Check: report.Check, Kind: rules.Kind(report.Kind)}

		if typ == ResourceVolume && isAnonymousName(report.Name) {
			item.Type = ResourceAnonymousVolume
		}

		// One line per tag: each reference is listed, and removed, on its own.
		if typ == ResourceImage {
			if err := e.imageLabels(ctx, &item, report.Tag); err != nil {
				return items, err
			}
		}

		items = append(items, item)
	}

	return items, nil
}

// imageLabels fills a listed image's name, check and kind from an inspect of its ID.
func (e *Engine) imageLabels(ctx context.Context, item *Listed, tag string) error {
	var report identified

	found, err := e.read(ctx, request{verb: verbImageInspect, args: []arg{{val: imageIDTemplate}, {val: item.ID}}},
		&report)
	if err != nil || !found {
		return err
	}

	item.Name = item.Name + ":" + tag
	item.Check, item.Kind = report.Labels[rules.LabelCheck], rules.Kind(report.Labels[rules.LabelKind])

	return nil
}

// isAnonymousName reports the engine's name for an anonymous volume: 64 lowercase hex digits.
func isAnonymousName(name string) bool {
	return len(name) == 64 && strings.Trim(name, "0123456789abcdef") == ""
}
