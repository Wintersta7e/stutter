package provision

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

// CleanOptions are `stutter clean`'s.
type CleanOptions struct {
	// Stdout receives one line per resource removed, or that would be, then one counts line.
	Stdout io.Writer
	// StateDir holds the check ledgers; empty is the default Open uses.
	StateDir string
	// CheckID limits the clean to that check, and additionally removes resources carrying exactly
	// its check label that no ledger here names.
	CheckID string
	// DryRun prints what would be removed and removes nothing.
	DryRun bool
}

// CleanResult is what Clean found and did.
type CleanResult struct {
	// Unledgered lists, by check ID, labelled resources no ledger here names: never removed without
	// CheckID.
	Unledgered map[string][]Listed
	// Removed are the resources removed — or, in a dry run, that would be.
	Removed []Listed
	// Failed are the resources that could not be verified or removed.
	Failed []Listed
	// Live are the check IDs whose owner still holds its ledger: skipped.
	Live []string
	// Foreign are the check IDs of ledgers of another engine or host: listed, never acted on.
	Foreign []string
	// Corrupt are the check IDs of ledgers that could not be read whole.
	Corrupt []string
}

// cleanDeps is what Clean reads from the host, as fields so tests can stand in for the engine.
type cleanDeps struct {
	host  hostFS
	admit func(ctx context.Context) (Identity, engineCaller, error)
	env   []string
}

// Clean removes what dead checks on this engine and host left: every resource their ledgers record,
// verified against their own check, their whole private directories — kept and retained ones
// included — and the ledgers. It writes no ledger, mints no check and creates nothing; the engine's
// client runs with a configuration directory that is never created.
func Clean(ctx context.Context, opts CleanOptions) (CleanResult, error) {
	return cleanWith(ctx, opts, cleanDeps{
		admit: func(ctx context.Context) (Identity, engineCaller, error) {
			identity, runner, err := preconditionsIn(ctx, os.Environ(), string(filepath.Separator))
			if err != nil {
				return Identity{}, nil, err
			}

			return identity, runner, nil
		},
		env:  os.Environ(),
		host: defaultHostFS(),
	})
}

// cleaner is one clean's state.
type cleaner struct {
	engine   *Engine
	out      io.Writer
	recorded map[string]bool
	result   CleanResult
	opts     CleanOptions
	hostName string
	lines    []string
	errs     []error
}

func cleanWith(ctx context.Context, opts CleanOptions, deps cleanDeps) (CleanResult, error) {
	identity, run, err := deps.admit(ctx)
	if err != nil {
		return CleanResult{}, err
	}

	suffix, err := newCheckID()
	if err != nil {
		return CleanResult{}, err
	}

	run.attach(filepath.Join(os.TempDir(), "stutter-clean-"+suffix[:16]+"-absent"), nil, !opts.DryRun)

	hostName, err := os.Hostname()
	if err != nil {
		return CleanResult{}, fmt.Errorf("%w: read the host name: %w", ErrPrecondition, err)
	}

	c := &cleaner{
		engine: &Engine{run: run, host: deps.host, identity: identity}, out: opts.Stdout, opts: opts,
		hostName: hostName, recorded: map[string]bool{}, result: CleanResult{Unledgered: map[string][]Listed{}},
	}

	if c.out == nil {
		c.out = io.Discard
	}

	ledgered, err := c.cleanLedgers(ctx, deps)
	if err != nil {
		return c.result, err
	}

	if opts.CheckID != "" && slices.Contains(c.result.Live, opts.CheckID) {
		return c.result, fmt.Errorf("%w: check %s", ErrLive, opts.CheckID)
	}

	if opts.CheckID != "" {
		c.cleanLabelled(ctx)
	} else {
		unledgered, err := c.engine.unledgered(ctx, ledgered)
		c.result.Unledgered, c.errs = unledgered, append(c.errs, err)
	}

	return c.result, errors.Join(c.print(), c.failure())
}

// cleanLedgers cleans every ledger in the state directory, or only CheckID's, and returns the check
// IDs that have a ledger here. A state directory that does not exist holds none.
func (c *cleaner) cleanLedgers(ctx context.Context, deps cleanDeps) (map[string]bool, error) {
	dir, err := statePath(c.opts.StateDir, deps.env)
	if err != nil {
		return nil, err
	}

	if _, missing := deps.host.lstat(dir); errors.Is(missing, fs.ErrNotExist) {
		return map[string]bool{}, nil
	}

	if refused := deps.host.private(dir); refused != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrStateDir, dir, refused)
	}

	paths, err := filesEnding(dir, ".ledger")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStateDir, err)
	}

	ledgered := map[string]bool{}

	for _, path := range paths {
		check := checkOf(path)
		ledgered[check] = true

		if c.opts.CheckID == "" || check == c.opts.CheckID {
			c.cleanLedger(ctx, path, check)
		}
	}

	return ledgered, nil
}

// cleanLedger cleans one ledger whose owner is dead: its lock is free.
func (c *cleaner) cleanLedger(ctx context.Context, path, check string) {
	loaded, err := loadLedger(path)

	switch {
	case err != nil || loaded.corrupt:
		c.result.Corrupt = append(c.result.Corrupt, check)

		return
	case loaded.header.EngineID != c.engine.identity.EngineID || loaded.header.Host != c.hostName:
		c.result.Foreign = append(c.result.Foreign, check)

		return
	default:
	}

	led, locked := c.engine.lockLedger(path)
	if !locked {
		c.result.Live = append(c.result.Live, check)

		return
	}

	b, err := reloadBook(led)
	if err != nil {
		c.result.Corrupt = append(c.result.Corrupt, check)
		c.errs = append(c.errs, led.close())

		return
	}

	for _, rec := range b.all() {
		c.recorded[rec.id], c.recorded[rec.name] = true, true
	}

	if c.opts.DryRun {
		c.wouldRemove(b)
		c.errs = append(c.errs, led.close())

		return
	}

	if c.removeLedgered(ctx, b) {
		c.errs = append(c.errs, led.remove())
	} else {
		c.errs = append(c.errs, led.close())
	}
}

// removeLedgered removes every engine resource b records and its whole private directory, and
// reports whether nothing of the check is left.
func (c *cleaner) removeLedgered(ctx context.Context, b *book) bool {
	clean := true

	for _, typ := range []ResourceType{ResourceContainer, ResourceNetwork, ResourceVolume, ResourceImage} {
		for _, rec := range b.newestFirst(typ) {
			clean = c.settle(ctx, b, rec) && clean
		}
	}

	path, exists, err := c.engine.recordedPrivate(b)
	if err == nil && exists {
		err = removeMade(path)
	}

	private := Listed{Type: ResourceHostPath, ID: path, Name: path, Check: b.check}

	if err != nil {
		c.fail(private, err)

		return false
	}

	if exists {
		c.removed(private)
	}

	return clean
}

// settle removes one recorded resource — or resolves its intent — and reports whether it is gone.
func (c *cleaner) settle(ctx context.Context, b *book, rec record) bool {
	if rec.state == opRemoved {
		return true
	}

	settle := c.engine.removeRecorded
	if rec.state == opIntent || rec.state == opAbsent {
		settle = c.engine.resolveIntent
	}

	listed := listedOf(rec, b.check)
	listed.Type = typeOfRecord(rec)

	if err := settle(ctx, b, rec); err != nil {
		c.fail(listed, err)

		return false
	}

	if now, _ := b.record(rec.seq); now.state == opRemoved {
		c.removed(listed)
	}

	return true
}

// wouldRemove names what a real clean of b would remove.
func (c *cleaner) wouldRemove(b *book) {
	for _, rec := range b.all() {
		if rec.typ != ResourceHostPath && rec.state != opRemoved && rec.state != opAbsent {
			listed := listedOf(rec, b.check)
			listed.Type = typeOfRecord(rec)
			c.removed(listed)
		}
	}

	if path, exists, err := c.engine.recordedPrivate(b); err == nil && exists {
		c.removed(Listed{Type: ResourceHostPath, ID: path, Name: path, Check: b.check})
	}
}

// cleanLabelled removes what carries exactly CheckID's label and a kind of the vocabulary, and that
// no ledger here names — images only under that check's reserved references. The user typing the
// check ID is the authority; each is still inspected before its removal.
func (c *cleaner) cleanLabelled(ctx context.Context) {
	check := c.opts.CheckID
	items, err := c.engine.listLabelled(ctx, rules.LabelCheck+"="+check)
	c.errs = append(c.errs, err)

	b := newBook(nil, check)

	for i, item := range items {
		if c.recorded[item.ID] || c.recorded[item.Name] || item.Check != check ||
			!slices.Contains(rules.Kinds(), item.Kind) ||
			item.Type == ResourceImage && !strings.HasPrefix(item.Name, rules.ImageDomain+"/"+check+"/") {
			continue
		}

		if c.opts.DryRun {
			c.removed(item)

			continue
		}

		rec := record{
			seq: i + 1, typ: item.Type, kind: item.Kind, name: item.Name, id: item.ID, state: opVerified,
		}

		if item.Type == ResourceAnonymousVolume {
			rec.typ = ResourceVolume
		}

		b.fold(entry{Seq: rec.seq, Op: opVerified, Type: rec.typ, Kind: rec.kind, Name: rec.name, ID: rec.id})
		c.settle(ctx, b, rec)
	}
}

func (c *cleaner) removed(item Listed) {
	c.result.Removed = append(c.result.Removed, item)

	verb := "removed"
	if c.opts.DryRun {
		verb = "would remove"
	}

	c.lines = append(c.lines, fmt.Sprintf("%s %s %s %s check=%s", verb, item.Type, item.ID, item.Name, item.Check))
}

func (c *cleaner) fail(item Listed, err error) {
	c.result.Failed = append(c.result.Failed, item)
	c.errs = append(c.errs, err)
	c.lines = append(c.lines, fmt.Sprintf("failed %s %s %s check=%s", item.Type, item.ID, item.Name, item.Check))
}

// print writes every line, the unledgered listing, the skipped ledgers and the counts.
func (c *cleaner) print() error {
	lines := c.lines

	for check, items := range c.result.Unledgered {
		for _, item := range items {
			lines = append(lines, fmt.Sprintf("unledgered check=%s %s %s %s", check, item.Type, item.ID, item.Name))
		}
	}

	for _, group := range []struct {
		word   string
		checks []string
	}{{"live", c.result.Live}, {"foreign", c.result.Foreign}, {"corrupt", c.result.Corrupt}} {
		for _, check := range group.checks {
			lines = append(lines, group.word+" check="+check)
		}
	}

	lines = append(lines, fmt.Sprintf("clean removed=%d failed=%d live=%d foreign=%d corrupt=%d unledgered=%d",
		len(c.result.Removed), len(c.result.Failed), len(c.result.Live), len(c.result.Foreign),
		len(c.result.Corrupt), len(c.result.Unledgered)))

	if _, err := io.WriteString(c.out, strings.Join(lines, "\n")+"\n"); err != nil {
		return fmt.Errorf("write the clean listing: %w", err)
	}

	return nil
}

// failure is Clean's error: every refusal and failure, joined, wrapping ErrEngine.
func (c *cleaner) failure() error {
	if err := errors.Join(c.errs...); err != nil {
		return fmt.Errorf("%w: %w", ErrEngine, err)
	}

	return nil
}
