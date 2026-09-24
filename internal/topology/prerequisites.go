package topology

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
)

// osRelease names the running kernel; a WSL kernel says so.
const osRelease = "/proc/sys/kernel/osrelease"

// ErrPrerequisite means this host cannot run a compose check, before anything was created.
var ErrPrerequisite = errors.New("this host cannot run a compose check")

// prerequisites is the host check with its kernel file as a parameter, for the internal test.
type prerequisites struct {
	osRelease string
}

// Prerequisites refuses a host whose networking the topology cannot use, before any Docker resource
// exists: WSL in mirrored networking mode, where containers cannot reach the host the way the relays
// need. networkingMode is asked only on a WSL kernel; a mode that cannot be read is refused too.
func Prerequisites(ctx context.Context, networkingMode func(context.Context) (string, error)) error {
	return prerequisites{osRelease: osRelease}.check(ctx, networkingMode)
}

func (p prerequisites) check(ctx context.Context, networkingMode func(context.Context) (string, error)) error {
	release, err := os.ReadFile(p.osRelease)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("%w: read the kernel release: %w", ErrPrerequisite, err)
	}

	if !strings.Contains(strings.ToLower(string(release)), "microsoft") {
		return nil
	}

	mode, err := networkingMode(ctx)
	if err != nil {
		return fmt.Errorf("%w: WSL's networking mode is unreadable (wslinfo): %w", ErrPrerequisite, err)
	}

	if mode = strings.TrimSpace(mode); mode == "mirrored" {
		return fmt.Errorf("%w: WSL runs in %s networking mode; --compose needs WSL's NAT networking mode",
			ErrPrerequisite, mode)
	}

	return nil
}
