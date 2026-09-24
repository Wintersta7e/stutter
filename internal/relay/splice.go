package relay

import (
	"errors"
	"io"
	"net"
	"sync"
)

// spliceBuffer is one read's worth of bytes. It bounds a read, nothing else: every read is written
// on in full before the next.
const spliceBuffer = 32 << 10

// Splice copies a to b and b to a until both directions are done, keeping the two ways a TCP
// connection can end distinct.
//
// EOF on one leg is a half-close: the other leg's write side is closed and the reverse direction keeps
// flowing, because a client that shuts its write side still waits for the reply. A read error other
// than EOF, or any write error, is a reset: both legs are closed abortively so each far end reads a
// reset too. Converting either ending into the other would hand the service a behaviour its real
// dependency never had. When both directions ended cleanly, both legs are closed.
func Splice(a, b net.Conn) {
	var (
		once    sync.Once
		aborted bool
	)

	abortBoth := func() {
		once.Do(func() {
			aborted = true

			// Both legs are already failing; a close that fails too has no one left to tell.
			_ = reset(a) //nolint:errcheck // see above.
			_ = reset(b) //nolint:errcheck // see above.
		})
	}

	var pipes sync.WaitGroup

	pipes.Go(func() { pipe(b, a, abortBoth) })
	pipe(a, b, abortBoth)
	pipes.Wait()

	// Both pipes have returned, so abortBoth can no longer run: aborted is settled.
	if !aborted {
		_ = a.Close()
		_ = b.Close()
	}
}

// pipe copies src to dst, each read written straight through, until src ends.
func pipe(dst, src net.Conn, abortBoth func()) {
	buffer := make([]byte, spliceBuffer)

	for {
		read, err := src.Read(buffer)
		if read > 0 {
			if _, writeErr := dst.Write(buffer[:read]); writeErr != nil {
				abortBoth()

				return
			}
		}

		if err == nil {
			continue
		}

		if errors.Is(err, io.EOF) && closeWrite(dst) == nil {
			return
		}

		abortBoth()

		return
	}
}
