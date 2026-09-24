package provision

import (
	"archive/tar"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// The proofs a stopped seed must pass before its snapshot is committed. A commit silently omits
// whatever lives on a volume, and a SIGKILLed postmaster leaves a stale pid file: either restores
// healthy, runs its init again, and has lost every row the jobs wrote.

// errGuard means a dependency's container failed a proof of its storage: a stopped seed before its
// snapshot, or a restore before its start.
var errGuard = errors.New("a storage proof failed")

// The files of a Postgres data directory the guard reads.
const (
	pgVersionFile  = "PG_VERSION"
	pgControlFile  = "global/pg_control"
	postmasterFile = "postmaster.pid"
	// identifierBytes is pg_control's leading system_identifier, little-endian.
	identifierBytes = 8
	// readLimit bounds what the guard reads of one file: both are a few kilobytes at most.
	readLimit = 1 << 20
)

// dataDirectory is what the Postgres guard read of a data directory.
type dataDirectory struct {
	files map[string][]byte
}

// readDataDirectory reads the tar of a data directory as the engine's copy streams it: every entry
// below the directory's own name. Only the files the guard reads keep their content.
func readDataDirectory(stream io.Reader) (dataDirectory, error) {
	archive := tar.NewReader(stream)
	dir := dataDirectory{files: map[string][]byte{}}

	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			return dir, nil
		}

		if err != nil {
			return dataDirectory{}, fmt.Errorf("%w: the data directory is unreadable: %w", errGuard, err)
		}

		_, rel, found := strings.Cut(strings.TrimPrefix(path.Clean(header.Name), "/"), "/")
		if !found {
			continue
		}

		var content []byte

		if slices.Contains([]string{pgVersionFile, pgControlFile}, rel) {
			if content, err = io.ReadAll(io.LimitReader(archive, readLimit)); err != nil {
				return dataDirectory{}, fmt.Errorf("%w: read %s: %w", errGuard, rel, err)
			}
		}

		dir.files[rel] = content
	}
}

// pgGuard proves a stopped Postgres seed's data directory is in its container layer and was shut down
// cleanly, and returns the cluster's system identifier, which every restore must carry.
func pgGuard(dir dataDirectory) (uint64, error) {
	if _, ok := dir.files[pgVersionFile]; !ok {
		return 0, fmt.Errorf("%w: %s holds no %s: the cluster is not in the container layer", errGuard, pgdataPath,
			pgVersionFile)
	}

	if _, ok := dir.files[postmasterFile]; ok {
		return 0, fmt.Errorf("%w: %s still holds %s: the cluster was not shut down cleanly", errGuard, pgdataPath,
			postmasterFile)
	}

	control, ok := dir.files[pgControlFile]
	if !ok || len(control) < identifierBytes {
		return 0, fmt.Errorf("%w: %s holds no readable %s", errGuard, pgdataPath, pgControlFile)
	}

	return binary.LittleEndian.Uint64(control[:identifierBytes]), nil
}

// mountGuard proves what a stopped seed mounts: nothing over the relocated data directory or a parent
// of it, every template the plan made mounted at its target from the check's own volume, and — on the
// other path — nothing else but a tmpfs or a read-only bind, every image VOLUME covered, and no
// anonymous volume.
func mountGuard(got []compose.InspectedMount, plan storagePlan, templates []*Volume, imageVolumes []string) error {
	ours := map[string]bool{}
	for _, volume := range templates {
		ours[volume.Name()] = true
	}

	covered := map[string]string{}

	for _, m := range got {
		target := path.Clean(m.Destination)

		if plan.pgdata && under(pgdataPath, []string{target}) {
			return fmt.Errorf("%w: a mount at %s covers the cluster at %s", errGuard, target, pgdataPath)
		}

		if err := admitted(m, ours); err != nil {
			return err
		}

		covered[target] = m.Type
	}

	return coverage(covered, plan, imageVolumes)
}

// admitted refuses a mount that is neither a template of this check, a tmpfs nor a read-only bind.
func admitted(m compose.InspectedMount, ours map[string]bool) error {
	switch {
	case m.Type == mountVolume && ours[m.Name]:
		return nil
	case m.Type == mountVolume:
		return fmt.Errorf("%w: %s is mounted from a volume that is not a template of this check", errGuard,
			m.Destination)
	case m.Type == mountBind && m.RW:
		return fmt.Errorf("%w: %s is a writable bind", errGuard, m.Destination)
	case m.Type == mountBind, m.Type == mountTmpfs:
		return nil
	default:
		return fmt.Errorf("%w: %s is a %s mount", errGuard, m.Destination, m.Type)
	}
}

// coverage proves every template target is a volume of this check, and — where the plan keeps no
// cluster in the layer — every image VOLUME is covered by a template or a tmpfs.
func coverage(covered map[string]string, plan storagePlan, imageVolumes []string) error {
	for _, tmpl := range plan.templates {
		if covered[path.Clean(tmpl.target)] != mountVolume {
			return fmt.Errorf("%w: %s is not on its template volume", errGuard, tmpl.target)
		}
	}

	if plan.pgdata {
		return nil
	}

	for _, volume := range imageVolumes {
		if kind := covered[path.Clean(volume)]; kind != mountVolume && kind != mountTmpfs {
			return fmt.Errorf("%w: the image VOLUME %s is covered by no template", errGuard, volume)
		}
	}

	return nil
}
