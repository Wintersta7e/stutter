//go:build !linux

package relay

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"runtime"
)

// errCatchAllOS means the catch-all needs Linux's readiness loop. A relay only ever runs in a Linux
// container, so this is reached by nothing but a build for another system.
var errCatchAllOS = errors.New("the catch-all listener runs only on linux")

// catchAll has no implementation off Linux.
type catchAll struct{}

func openCatchAll(netip.Addr, []uint16) (*catchAll, error) {
	return nil, fmt.Errorf("%w, not %s", errCatchAllOS, runtime.GOOS)
}

func (*catchAll) serve(func(conn net.Conn, port uint16)) error { return errCatchAllOS }

func (*catchAll) wake() {}

func (*catchAll) close() {}
