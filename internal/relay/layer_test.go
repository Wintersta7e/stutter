package relay_test

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/relay"
)

func readLayer(t *testing.T, executable string) []byte {
	t.Helper()

	layer, err := relay.Layer(executable)
	if err != nil {
		t.Fatalf("Layer() error = %v", err)
	}

	defer func() { _ = layer.Close() }()

	content, err := io.ReadAll(layer)
	if err != nil {
		t.Fatalf("read the layer: %v", err)
	}

	return content
}

// TestLayerIsDeterministic keeps the relay image a function of the binary alone: two checks of one
// binary build the same layer, whatever the clock or the file's own metadata say.
func TestLayerIsDeterministic(t *testing.T) {
	t.Parallel()

	executable := filepath.Join(t.TempDir(), "stutter-build")
	binary := bytes.Repeat([]byte("\x7fELF relay "), 4096)

	if err := os.WriteFile(executable, binary, 0o600); err != nil {
		t.Fatalf("write the executable: %v", err)
	}

	first := readLayer(t, executable)

	if err := os.Chtimes(executable, time.Now(), time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("touch the executable: %v", err)
	}

	second := readLayer(t, executable)

	if !bytes.Equal(first, second) {
		at := 0
		for at < min(len(first), len(second)) && first[at] == second[at] {
			at++
		}

		t.Errorf("the two layers differ at byte %d", at)
	}

	reader := tar.NewReader(bytes.NewReader(first))

	header, err := reader.Next()
	if err != nil {
		t.Fatalf("read the layer's entry: %v", err)
	}

	if header.Name != "stutter" || header.Typeflag != tar.TypeReg || header.Mode != 0o555 ||
		header.Uid != 0 || header.Gid != 0 || header.Uname != "" || header.Gname != "" ||
		!header.ModTime.Equal(time.Unix(0, 0)) {
		t.Errorf("entry = %+v, want stutter, a regular file, 0555, uid/gid 0, no names, mtime 0", header)
	}

	content, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(content, binary) {
		t.Errorf("the entry's content is not the executable (%v)", err)
	}

	if _, err := reader.Next(); err != io.EOF {
		t.Errorf("a second entry follows (%v), want exactly one", err)
	}
}

func TestImageChangesSetTheUserAndEntrypoint(t *testing.T) {
	t.Parallel()

	want := []string{"USER 65534:65534", `ENTRYPOINT ["/stutter","relay"]`}
	if got := relay.ImageChanges(); !slices.Equal(got, want) {
		t.Errorf("ImageChanges() = %q, want %q", got, want)
	}
}
