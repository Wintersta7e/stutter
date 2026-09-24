package provision

import (
	"errors"
	"net"
	"strconv"
	"testing"
)

// TestTheHostPortBandIsBelowBothEphemeralRanges: a published port is chosen below where the kernel's
// ephemeral range starts and below Windows' dynamic range, from the floor while that leaves room, else
// from the first unprivileged port; a range that leaves no unprivileged port below it is refused.
func TestTheHostPortBandIsBelowBothEphemeralRanges(t *testing.T) {
	t.Parallel()

	rows := []struct {
		name      string
		want      portBand
		ephemeral int
		refused   bool
	}{
		{name: "the Linux default", ephemeral: 32768, want: portBand{lo: 20000, hi: 32767}},
		{name: "a range above Windows' dynamic range", ephemeral: 60000, want: portBand{lo: 20000, hi: 49151}},
		{name: "a range starting just above the floor", ephemeral: 20001, want: portBand{lo: 20000, hi: 20000}},
		{name: "a range starting low", ephemeral: 10000, want: portBand{lo: 1024, hi: 9999}},
		{name: "a range starting at the floor", ephemeral: 20000, want: portBand{lo: 1024, hi: 19999}},
		{name: "a range leaving nothing unprivileged", ephemeral: 1024, refused: true},
	}

	t.Logf("%d rows", len(rows))

	for _, row := range rows {
		band, err := hostPortBand(row.ephemeral)

		if row.refused {
			if !errors.Is(err, ErrNoHostPort) {
				t.Errorf("%s: band = %+v, %v; want ErrNoHostPort", row.name, band, err)
			}

			continue
		}

		if err != nil || band != row.want {
			t.Errorf("%s: band = %+v, %v; want %+v", row.name, band, err, row.want)
		}
	}
}

// TestTheEphemeralRangeIsReadAsTheKernelWritesIt: two numbers, tab-separated, newline-terminated; any
// other shape is refused rather than guessed at.
func TestTheEphemeralRangeIsReadAsTheKernelWritesIt(t *testing.T) {
	t.Parallel()

	if start, err := parsePortRange("32768\t60999\n"); err != nil || start != 32768 {
		t.Errorf("parsePortRange(kernel shape) = %d, %v; want 32768", start, err)
	}

	for _, bad := range []string{"", "32768", "x\t60999\n", "0\t60999\n", "70000\t70001\n"} {
		if start, err := parsePortRange(bad); !errors.Is(err, ErrNoHostPort) {
			t.Errorf("parsePortRange(%q) = %d, %v; want ErrNoHostPort", bad, start, err)
		}
	}
}

// TestAReservedHostPortIsNeverSelectedTwice: two creates in one process choosing before either starts
// get different ports, each free on loopback and inside the band, and a released port may be chosen
// again.
func TestAReservedHostPortIsNeverSelectedTwice(t *testing.T) {
	t.Parallel()

	first, err := ReserveHostPort(t.Context())
	if err != nil {
		t.Fatalf("ReserveHostPort() error = %v", err)
	}

	second, err := ReserveHostPort(t.Context(), first)
	if err != nil {
		t.Fatalf("ReserveHostPort() error = %v", err)
	}

	defer ReleaseHostPort(second)

	if first == second {
		t.Errorf("both creates were given port %d", first)
	}

	start, err := ephemeralStart()
	if err != nil {
		t.Fatalf("ephemeralStart() error = %v", err)
	}

	band, err := hostPortBand(start)
	if err != nil {
		t.Fatalf("hostPortBand(%d) error = %v", start, err)
	}

	for _, port := range []uint16{first, second} {
		if int(port) < band.lo || int(port) > band.hi {
			t.Errorf("port %d is outside the band %+v", port, band)
		}

		if !reservedHostPort(port) {
			t.Errorf("port %d is not reserved after its selection", port)
		}
	}

	ReleaseHostPort(first)

	if reservedHostPort(first) {
		t.Errorf("port %d is still reserved after its release", first)
	}
}

// TestAHostPortSomethingListensOnIsNotFree: a port a host process listens on is not free, whoever
// else would publish on it.
func TestAHostPortSomethingListensOnIsNotFree(t *testing.T) {
	t.Parallel()

	var config net.ListenConfig

	listener, err := config.Listen(t.Context(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	defer func() { _ = listener.Close() }()

	_, text, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort() error = %v", err)
	}

	port, err := strconv.ParseUint(text, 10, 16)
	if err != nil {
		t.Fatalf("ParseUint() error = %v", err)
	}

	if freeOnLoopback(t.Context(), uint16(port)) {
		t.Errorf("port %d is taken by a listener but reads as free", port)
	}
}
