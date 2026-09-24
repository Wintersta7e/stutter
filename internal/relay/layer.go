package relay

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	// layerEntry is the one file in the relay image, at /stutter.
	layerEntry = "stutter"
	// layerMode is read and execute for everyone, write for no one: the relay runs as an unprivileged
	// user, from a read-only filesystem.
	layerMode = 0o555
)

// ImageChanges is the relay image's configuration: an unprivileged user, and the binary's hidden relay
// command as the entrypoint. Nothing else — no command, environment, volume, port or healthcheck.
func ImageChanges() []string {
	return []string{"USER 65534:65534", `ENTRYPOINT ["/stutter","relay"]`}
}

// Layer streams the relay image's one layer: a tar holding executable as /stutter, owned by root, mode
// 0555, with a zero modification time. Nothing in it depends on the file's own metadata or the clock,
// so one binary always makes the same layer.
func Layer(executable string) (io.ReadCloser, error) {
	file, err := os.Open(executable)
	if err != nil {
		return nil, fmt.Errorf("open the relay binary: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("stat the relay binary: %w", err)
	}

	reader, writer := io.Pipe()

	go func() {
		defer func() { _ = file.Close() }()

		writer.CloseWithError(writeLayer(writer, file, info.Size()))
	}()

	return reader, nil
}

// writeLayer writes the layer's tar to w.
func writeLayer(w io.Writer, content io.Reader, size int64) error {
	archive := tar.NewWriter(w)

	err := archive.WriteHeader(&tar.Header{
		Name:     layerEntry,
		Typeflag: tar.TypeReg,
		Mode:     layerMode,
		Size:     size,
		ModTime:  time.Unix(0, 0),
		Format:   tar.FormatUSTAR,
	})
	if err == nil {
		_, err = io.Copy(archive, content)
	}

	if closeErr := archive.Close(); err == nil {
		err = closeErr
	}

	if err != nil {
		return fmt.Errorf("write the relay layer: %w", err)
	}

	return nil
}
