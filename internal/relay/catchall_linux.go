//go:build linux

package relay

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"syscall"
)

// epollBatch is how many ready sockets one wait reports at most. More are simply reported by the next.
const epollBatch = 64

// catchAll is every catch-all socket, served by one readiness loop.
//
// A stub relay binds tens of thousands of them. One goroutine per socket measured 836 ms and 461 MiB
// to bind 64,510 ports; one loop over non-blocking sockets, 539 ms and 261 MiB.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type catchAll struct {
	// ports maps a listening socket to the port it listens on: the service's original destination.
	ports     map[int32]uint16
	sockets   []int
	epoll     int
	wakeRead  int
	wakeWrite int
}

// openCatchAll binds every port in ports on self, non-blocking, and registers each with the loop. A
// port that cannot be bound fails the whole catch-all, naming the port and the cause.
func openCatchAll(self netip.Addr, ports []uint16) (*catchAll, error) {
	epoll, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("create the catch-all's readiness loop: %w", err)
	}

	sockets := &catchAll{
		ports: make(map[int32]uint16, len(ports)), epoll: epoll, wakeRead: -1, wakeWrite: -1,
		sockets: make([]int, 0, len(ports)),
	}

	if err := sockets.open(self, ports); err != nil {
		sockets.close()

		return nil, err
	}

	return sockets, nil
}

func (c *catchAll) open(self netip.Addr, ports []uint16) error {
	var wake [2]int

	if err := syscall.Pipe2(wake[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		return fmt.Errorf("create the catch-all's wake pipe: %w", err)
	}

	c.wakeRead, c.wakeWrite = wake[0], wake[1]

	if err := c.watch(c.wakeRead); err != nil {
		return err
	}

	for _, port := range ports {
		socket, err := listenOn(self, port)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", netip.AddrPortFrom(self, port), err)
		}

		c.sockets = append(c.sockets, socket)
		c.ports[int32(socket)] = port //nolint:gosec // a file descriptor fits in the int32 epoll carries.

		if err := c.watch(socket); err != nil {
			return err
		}
	}

	return nil
}

// listenOn opens one non-blocking listening socket.
func listenOn(self netip.Addr, port uint16) (int, error) {
	socket, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open a socket: %w", err)
	}

	listened := errors.Join(
		syscall.SetsockoptInt(socket, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1),
		syscall.Bind(socket, &syscall.SockaddrInet4{Port: int(port), Addr: self.As4()}),
	)
	if listened == nil {
		listened = syscall.Listen(socket, syscall.SOMAXCONN)
	}

	if listened != nil {
		_ = syscall.Close(socket)

		return -1, listened //nolint:wrapcheck // the caller names the address; the cause reads best bare.
	}

	return socket, nil
}

func (c *catchAll) watch(fd int) error {
	event := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(fd)} //nolint:gosec // see open.
	if err := syscall.EpollCtl(c.epoll, syscall.EPOLL_CTL_ADD, fd, &event); err != nil {
		return fmt.Errorf("watch a catch-all socket: %w", err)
	}

	return nil
}

// serve waits on every socket at once and hands each accepted connection to handle, with the port it
// arrived on. It returns nil once woken, or the first failure.
func (c *catchAll) serve(handle func(conn net.Conn, port uint16)) error {
	events := make([]syscall.EpollEvent, epollBatch)

	for {
		ready, err := syscall.EpollWait(c.epoll, events, -1)
		if errors.Is(err, syscall.EINTR) {
			continue
		}

		if err != nil {
			return fmt.Errorf("wait on the catch-all: %w", err)
		}

		for _, event := range events[:ready] {
			if int(event.Fd) == c.wakeRead {
				return nil
			}

			if err := acceptAll(int(event.Fd), c.ports[event.Fd], handle); err != nil {
				return err
			}
		}
	}
}

// acceptAll accepts every connection waiting on one socket.
func acceptAll(socket int, port uint16, handle func(conn net.Conn, port uint16)) error {
	for {
		accepted, _, err := syscall.Accept4(socket, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)

		switch {
		case err == nil:
		case errors.Is(err, syscall.EAGAIN):
			return nil
		case errors.Is(err, syscall.EINTR), errors.Is(err, syscall.ECONNABORTED):
			continue
		default:
			return fmt.Errorf("accept on port %d: %w", port, err)
		}

		conn, err := fileConn(accepted)
		if err != nil {
			return fmt.Errorf("accept on port %d: %w", port, err)
		}

		handle(conn, port)
	}
}

// fileConn turns an accepted descriptor into a net.Conn, which takes a copy of it.
func fileConn(fd int) (net.Conn, error) {
	file := os.NewFile(uintptr(fd), "catch-all")
	defer func() { _ = file.Close() }()

	conn, err := net.FileConn(file)
	if err != nil {
		return nil, fmt.Errorf("adopt an accepted connection: %w", err)
	}

	return conn, nil
}

// wake ends serve.
func (c *catchAll) wake() {
	_, _ = syscall.Write(c.wakeWrite, []byte{0}) //nolint:errcheck // a full pipe has already woken the loop.
}

// close releases every descriptor. It runs once serve has returned, or when serve never ran.
func (c *catchAll) close() {
	for _, socket := range c.sockets {
		_ = syscall.Close(socket)
	}

	for _, fd := range []int{c.wakeRead, c.wakeWrite, c.epoll} {
		if fd >= 0 {
			_ = syscall.Close(fd)
		}
	}
}
