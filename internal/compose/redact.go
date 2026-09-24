package compose

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Format prints the spec's shape — service, image, environment keys, argument counts, mount and
// copy-in targets — for every verb, never a value. Without it `%+v` would print the environment,
// the arguments and, through reflection, the unexported compose environment.
func (s Spec) Format(f fmt.State, _ rune) {
	mounts := make([]string, 0, len(s.Mounts))
	for _, mount := range s.Mounts {
		mounts = append(mounts, string(mount.Kind)+":"+mount.Target)
	}

	replaced := make([]string, 0, len(s.Replaced))
	for _, entry := range s.Replaced {
		replaced = append(replaced, entry.Key)
	}

	fmt.Fprintf(f, "Spec{service=%s image=%s target=%t env=%v unset=%v entrypoint=%d cmd=%d "+
		"mounts=[%s] copyin=%v replaced=[%s] proxy-removed=%v ca-overridden=%v}",
		s.Service, s.Image, s.target, slices.Sorted(maps.Keys(s.Env)), s.Unset, len(s.Entrypoint), len(s.Cmd),
		strings.Join(mounts, " "), s.CopyIn, strings.Join(replaced, " "), s.ProxyRemoved, s.CAOverridden)
}

// Format prints the file's target, ownership, mode and size for every verb, never its content.
func (c CopyIn) Format(f fmt.State, _ rune) {
	fmt.Fprintf(f, "CopyIn{target=%s uid=%d gid=%d mode=%#o bytes=%d}", c.Target, c.UID, c.GID, c.Mode, len(c.data))
}
