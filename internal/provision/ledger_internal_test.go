//go:build linux

package provision

import (
	"bufio"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
)

func testHeader(id string) header {
	return header{
		Check: id, EngineID: "engine-a", Endpoint: "unix:///var/run/docker.sock", Context: "default",
		PrivateDir: filepath.Join(os.TempDir(), "stutter-"+id), Project: "stutter-" + id,
	}
}

// newTestLedger creates a ledger for testCheckID in a fresh state directory.
func newTestLedger(t *testing.T, host hostFS) *ledger {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "checks")

	state, err := stateDir(dir, nil, host)
	if err != nil {
		t.Fatalf("stateDir: %v", err)
	}

	led, err := createLedger(state, host, testHeader(testCheckID))
	if err != nil {
		t.Fatalf("createLedger: %v", err)
	}

	t.Cleanup(func() {
		if err := led.close(); err != nil {
			t.Logf("close ledger: %v", err)
		}
	})

	return led
}

// A sweep must never find a live ledger it could lock: the ledger is locked while still a temporary
// file, and only then renamed into place.
func TestALedgerIsLockedBeforeItIsVisible(t *testing.T) {
	t.Parallel()

	host := defaultHostFS()
	dir := filepath.Join(t.TempDir(), "checks")
	final := filepath.Join(dir, testCheckID+".ledger")
	checked := false

	realSync := host.sync
	host.sync = func(f *os.File) error {
		if !checked && strings.HasSuffix(f.Name(), ".ledger.tmp") {
			checked = true

			if _, err := os.Lstat(final); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("the ledger was visible before its header was synced: %v", err)
			}

			if lockable(t, f.Name()) {
				t.Error("the temporary ledger was not locked when its header was written")
			}
		}

		return realSync(f)
	}

	state, err := stateDir(dir, nil, host)
	if err != nil {
		t.Fatal(err)
	}

	led, err := createLedger(state, host, testHeader(testCheckID))
	if err != nil {
		t.Fatalf("createLedger: %v", err)
	}
	defer closeLedger(t, led)

	if !checked {
		t.Fatal("the header was never synced while the ledger was temporary")
	}

	if lockable(t, final) {
		t.Error("a second lock on the ledger was granted")
	}

	if _, err := os.Lstat(final + ".tmp"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the temporary ledger remains: %v", err)
	}
}

func closeLedger(t *testing.T, led *ledger) {
	t.Helper()

	if err := led.close(); err != nil {
		t.Errorf("close ledger: %v", err)
	}
}

// lockable reports whether a second open file description of path can take the lock.
func lockable(t *testing.T, path string) bool {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFile(t, file)

	got, err := defaultHostFS().tryLock(file)
	if err != nil {
		t.Fatal(err)
	}

	return got
}

func closeFile(t *testing.T, file *os.File) {
	t.Helper()

	if err := file.Close(); err != nil {
		t.Errorf("close %s: %v", file.Name(), err)
	}
}

// A kill mid-write leaves a partial final line; the ledger is still whole up to the line before it.
func TestATruncatedFinalLineIsDiscarded(t *testing.T) {
	t.Parallel()

	for name, tail := range map[string]string{
		"partial line":            `{"seq":3,"op":"int`,
		"unparseable final line":  "{\"seq\":3,\"op\":\n",
		"unknown op on last line": "{\"seq\":3,\"op\":\"exploded\",\"type\":\"volume\"}\n",
	} {
		led := newTestLedger(t, defaultHostFS())
		appendAll(t, led, entry{Seq: 1, Op: opIntent, Type: ResourceVolume, Kind: rules.KindSeed, Name: "v"},
			entry{Seq: 1, Op: opCreated, Type: ResourceVolume, ID: "v"})
		appendRaw(t, led.path, tail)

		got, err := loadLedger(led.path)
		if err != nil {
			t.Fatalf("%s: load: %v", name, err)
		}

		if got.corrupt || len(got.entries) != 2 || got.header.Check != testCheckID {
			t.Errorf("%s: corrupt=%v entries=%d header=%q; want the two whole entries", name, got.corrupt,
				len(got.entries), got.header.Check)
		}
	}
}

// A malformed line anywhere but the end is not a kill mid-write: the ledger is corrupt and nothing
// acts on it automatically.
func TestAMalformedLineElsewhereMakesTheLedgerCorrupt(t *testing.T) {
	t.Parallel()

	for name, middle := range map[string]string{
		"garbage":       "not json\n",
		"unknown op":    "{\"seq\":1,\"op\":\"exploded\",\"type\":\"volume\"}\n",
		"unknown field": "{\"seq\":1,\"op\":\"intent\",\"type\":\"volume\",\"force\":true}\n",
		"no type":       "{\"seq\":1,\"op\":\"intent\"}\n",
	} {
		led := newTestLedger(t, defaultHostFS())
		appendRaw(t, led.path, middle)
		appendAll(t, led, entry{Seq: 2, Op: opIntent, Type: ResourceVolume, Kind: rules.KindSeed, Name: "v"})

		got, err := loadLedger(led.path)
		if err != nil {
			t.Fatalf("%s: load: %v", name, err)
		}

		if !got.corrupt {
			t.Errorf("%s: a malformed middle line left the ledger uncorrupt", name)
		}
	}
}

// Every line is on disk before the engine call it precedes: one sync per line, taken after the line
// was written.
func TestEveryLineIsSyncedBeforeItReturns(t *testing.T) {
	t.Parallel()

	host := defaultHostFS()
	syncs := 0
	syncedSize := int64(-1)

	realSync := host.sync
	host.sync = func(f *os.File) error {
		if strings.Contains(f.Name(), ".ledger") {
			syncs++

			info, err := f.Stat()
			if err != nil {
				return err
			}

			syncedSize = info.Size()
		}

		return realSync(f)
	}

	led := newTestLedger(t, host)
	if syncs != 1 {
		t.Fatalf("the header took %d syncs, want 1", syncs)
	}

	for seq := 1; seq <= 3; seq++ {
		before := syncs

		appendAll(t, led, entry{Seq: seq, Op: opIntent, Type: ResourceNetwork, Kind: rules.KindNetwork, Name: "n"})

		info, err := os.Stat(led.path)
		if err != nil {
			t.Fatal(err)
		}

		if syncs != before+1 || syncedSize != info.Size() {
			t.Errorf("line %d: syncs %d→%d, synced %d of %d bytes", seq, before, syncs, syncedSize, info.Size())
		}
	}

	t.Logf("syncs=%d", syncs)
}

// A state directory on a network or 9p filesystem cannot be trusted for modes or cross-namespace
// locking, so it is refused before a ledger is created in it.
func TestANetworkFilesystemIsRefused(t *testing.T) {
	t.Parallel()

	magics := map[string]int64{
		"v9fs": 0x01021997, "nfs": 0x6969, "smb": 0x517B, "cifs": 0xFF534D42, "smb2": 0xFE534D42, "fuse": 0x65735546,
	}

	for name, magic := range magics {
		host := defaultHostFS()
		host.statfs = func(string) (int64, error) { return magic, nil }
		dir := filepath.Join(t.TempDir(), "checks")

		_, err := stateDir(dir, nil, host)
		if !errors.Is(err, ErrStateDir) || !strings.Contains(err.Error(), name) {
			t.Errorf("%s: stateDir = %v, want ErrStateDir naming the filesystem", name, err)
		}

		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}

		if len(entries) != 0 {
			t.Errorf("%s: the refused directory holds %d entries", name, len(entries))
		}
	}

	t.Logf("types=%d", len(magics))
}

// When a second lock through another open file description is granted, locks are not enforced here
// and the check must not sweep: it could not tell a live ledger from a dead one.
func TestAGrantedSecondLockDisablesTheSweep(t *testing.T) {
	t.Parallel()

	host := defaultHostFS()
	host.tryLock = func(*os.File) (bool, error) { return true, nil }

	led := newTestLedger(t, host)

	enforced, err := led.selfTest()
	if err != nil {
		t.Fatal(err)
	}

	if enforced {
		t.Error("a granted second lock was reported as enforced")
	}

	enforcing := newTestLedger(t, defaultHostFS())

	enforced, err = enforcing.selfTest()
	if err != nil || !enforced {
		t.Errorf("the real lock self-test = (%v, %v), want enforced", enforced, err)
	}
}

// Every real 9p mount on this host is refused by the same check a state or private directory gets.
// Where the host has none, the count printed is 0: that half is a local proof, not a CI one.
func TestARealNinePMountIsRefused(t *testing.T) {
	t.Parallel()

	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		t.Fatal(err)
	}
	defer closeFile(t, file)

	host := defaultHostFS()
	exercised := 0

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		found, ok := mountOf(scanner.Text())
		if !ok || found.fsType != "9p" {
			continue
		}

		exercised++

		if err := host.local(found.point); err == nil {
			t.Errorf("9p mount %s was accepted as local", found.point)
		}
	}

	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	t.Logf("9p mounts exercised=%d", exercised)
}

// mount is one mountinfo line's mount point and filesystem type.
type mount struct {
	point, fsType string
}

// mountOf reads a mountinfo line.
func mountOf(line string) (mount, bool) {
	before, after, found := strings.Cut(line, " - ")
	fields, rest := strings.Fields(before), strings.Fields(after)

	if !found || len(fields) < 5 || len(rest) == 0 {
		return mount{}, false
	}

	return mount{point: unescapeMount(fields[4]), fsType: rest[0]}, true
}

// unescapeMount decodes mountinfo's octal escapes (`\040` for a space).
func unescapeMount(text string) string {
	var out strings.Builder

	for i := 0; i < len(text); i++ {
		if text[i] == '\\' && i+3 < len(text) {
			if value, err := strconv.ParseUint(text[i+1:i+4], 8, 8); err == nil {
				out.WriteByte(byte(value))

				i += 3

				continue
			}
		}

		out.WriteByte(text[i])
	}

	return out.String()
}

// The ledgers live under XDG_STATE_HOME when it is absolute, else under HOME; with neither there is
// nowhere trustworthy to put them.
func TestTheStateDirectoryFollowsXDG(t *testing.T) {
	t.Parallel()

	xdg, home := t.TempDir(), t.TempDir()
	fromHome := filepath.Join(home, ".local", "state", "stutter", "checks")
	cases := []struct {
		want string
		env  []string
	}{
		{env: []string{"XDG_STATE_HOME=" + xdg, "HOME=" + home}, want: filepath.Join(xdg, "stutter", "checks")},
		{env: []string{"XDG_STATE_HOME=relative", "HOME=" + home}, want: fromHome},
		{env: []string{"HOME=" + home}, want: fromHome},
		{env: []string{"HOME=relative"}},
	}

	for _, tc := range cases {
		got, err := stateDir("", tc.env, defaultHostFS())

		switch {
		case tc.want == "" && !errors.Is(err, ErrStateDir):
			t.Errorf("%v: stateDir = (%q, %v), want ErrStateDir", tc.env, got, err)
		case tc.want != "" && (err != nil || got != tc.want):
			t.Errorf("%v: stateDir = (%q, %v), want %q", tc.env, got, err, tc.want)
		default:
		}
	}
}

// A state directory another user could read or write is refused, not tightened.
func TestAStateDirectoryWithTheWrongModeIsRefused(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "checks")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}

	//nolint:gosec // the test widens the mode on purpose, to see it refused.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	_, err := stateDir(dir, nil, defaultHostFS())
	if !errors.Is(err, ErrStateDir) || !strings.Contains(err.Error(), "0755") {
		t.Errorf("stateDir = %v, want ErrStateDir naming the mode", err)
	}
}

func appendAll(t *testing.T, led *ledger, entries ...entry) {
	t.Helper()

	for _, e := range entries {
		if err := led.append(e); err != nil {
			t.Fatalf("append %+v: %v", e, err)
		}
	}
}

// appendRaw writes text to the end of path as a crash or an editor would, bypassing the writer.
func appendRaw(t *testing.T, path, text string) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFile(t, file)

	if _, err := file.WriteString(text); err != nil {
		t.Fatal(err)
	}
}
