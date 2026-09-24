package provision

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// clusterIdentity is the system identifier the synthetic pg_control carries.
const clusterIdentity uint64 = 0x7a3c_1f00_dead_beef

// The guard tests' shared names.
const (
	socketDir = "/var/run/postgresql"
	cleanRow  = "clean"
)

// dataTar is a data directory as the engine's copy streams it: the directory's own entry, then each
// file below it.
func dataTar(t *testing.T, files map[string][]byte) *bytes.Buffer {
	t.Helper()

	var out bytes.Buffer

	archive := tar.NewWriter(&out)

	for _, dir := range []string{"pgdata/", "pgdata/global/"} {
		if err := archive.WriteHeader(&tar.Header{Name: dir, Typeflag: tar.TypeDir, Mode: 0o700}); err != nil {
			t.Fatal(err)
		}
	}

	for name, data := range files {
		header := &tar.Header{Name: "pgdata/" + name, Typeflag: tar.TypeReg, Mode: 0o600, Size: int64(len(data))}
		if err := archive.WriteHeader(header); err != nil {
			t.Fatal(err)
		}

		if _, err := archive.Write(data); err != nil {
			t.Fatal(err)
		}
	}

	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	return &out
}

// control is a pg_control whose first eight bytes are the identifier.
func control() []byte {
	return binary.LittleEndian.AppendUint64(nil, clusterIdentity)
}

// TestThePostgresGuardReadsTheDataDirectory refuses a data directory the snapshot would lose, or one
// left by a postmaster that did not shut down, and reads the identifier of a clean one.
func TestThePostgresGuardReadsTheDataDirectory(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		files map[string][]byte
		name  string
		want  string
	}{
		{name: "no PG_VERSION", want: pgVersionFile, files: map[string][]byte{pgControlFile: control()}},
		{name: "a stale pid file", want: postmasterFile, files: map[string][]byte{
			pgVersionFile: []byte("18\n"), pgControlFile: control(), postmasterFile: []byte("1\n"),
		}},
		{name: "a short pg_control", want: pgControlFile, files: map[string][]byte{
			pgVersionFile: []byte("18\n"), pgControlFile: {1, 2, 3},
		}},
		{name: cleanRow, files: map[string][]byte{pgVersionFile: []byte("18\n"), pgControlFile: control()}},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			dir, err := readDataDirectory(dataTar(t, row.files))
			if err != nil {
				t.Fatalf("readDataDirectory() error = %v", err)
			}

			identity, err := pgGuard(dir)

			switch {
			case row.want == "" && (err != nil || identity != clusterIdentity):
				t.Errorf("pgGuard() = %#x, %v; want %#x", identity, err, clusterIdentity)
			case row.want != "" && (!errors.Is(err, errGuard) || !strings.Contains(err.Error(), row.want)):
				t.Errorf("pgGuard() = %v, want a refusal naming %s", err, row.want)
			default:
			}
		})
	}
}

// volumeAt, tmpfsAt and bindAt are mounts a stopped seed's inspect reports.
func volumeAt(target, name string) compose.InspectedMount {
	return compose.InspectedMount{Type: mountVolume, Destination: target, Name: name, RW: true}
}

func tmpfsAt(target string) compose.InspectedMount {
	return compose.InspectedMount{Type: mountTmpfs, Destination: target, RW: true}
}

func bindAt(target string, writable bool) compose.InspectedMount {
	return compose.InspectedMount{Type: mountBind, Destination: target, Source: "/host" + target, RW: writable}
}

// TestThePostgresGuardRefusesAMountOverTheCluster refuses a seed whose cluster sits under a mount, and
// one whose planned template never reached its target.
func TestThePostgresGuardRefusesAMountOverTheCluster(t *testing.T) {
	t.Parallel()

	sock := &Volume{name: "template-sock"}
	plan := storagePlan{pgdata: true, templates: []templateMount{{target: socketDir}}}

	for _, row := range []struct {
		name string
		want string
		got  []compose.InspectedMount
	}{
		{name: "a mount over the cluster's parent", want: "/stutter", got: []compose.InspectedMount{
			tmpfsAt("/stutter"), volumeAt(socketDir, sock.name),
		}},
		{name: "a compose volume target left uncovered", want: socketDir, got: []compose.InspectedMount{
			tmpfsAt("/var/lib/postgresql"),
		}},
		{name: cleanRow, got: []compose.InspectedMount{
			tmpfsAt("/var/lib/postgresql"), volumeAt(socketDir, sock.name),
			bindAt("/docker-entrypoint-initdb.d", false),
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			err := mountGuard(row.got, plan, []*Volume{sock}, []string{"/var/lib/postgresql"})

			if row.want == "" && err != nil || row.want != "" && (!errors.Is(err, errGuard) ||
				!strings.Contains(err.Error(), row.want)) {
				t.Errorf("mountGuard() = %v, want %q named (nothing for clean)", err, row.want)
			}
		})
	}
}

// TestTheOtherGuardRequiresEveryVolumeCovered refuses a stopped seed that keeps anything a commit
// would drop or a restore would not reproduce: an uncovered VOLUME, an anonymous volume, a writable bind.
func TestTheOtherGuardRequiresEveryVolumeCovered(t *testing.T) {
	t.Parallel()

	data := &Volume{name: "template-data"}
	plan := storagePlan{templates: []templateMount{{target: "/data"}}}
	volumes := []string{"/data", logsVolume}

	for _, row := range []struct {
		name string
		want string
		got  []compose.InspectedMount
	}{
		{name: "an image VOLUME uncovered", want: logsVolume, got: []compose.InspectedMount{
			volumeAt("/data", data.name),
		}},
		{name: "an anonymous volume", want: logsVolume, got: []compose.InspectedMount{
			volumeAt("/data", data.name), volumeAt(logsVolume, "0123abcd"),
		}},
		{name: "a writable bind", want: "/conf", got: []compose.InspectedMount{
			volumeAt("/data", data.name), tmpfsAt(logsVolume), bindAt("/conf", true),
		}},
		{name: cleanRow, got: []compose.InspectedMount{
			volumeAt("/data", data.name), tmpfsAt(logsVolume), bindAt("/conf", false),
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			err := mountGuard(row.got, plan, []*Volume{data}, volumes)

			if row.want == "" && err != nil || row.want != "" && (!errors.Is(err, errGuard) ||
				!strings.Contains(err.Error(), row.want)) {
				t.Errorf("mountGuard() = %v, want %q named (nothing for clean)", err, row.want)
			}
		})
	}
}
