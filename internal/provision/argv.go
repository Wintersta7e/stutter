package provision

import (
	"net/netip"
	"slices"
)

// Pure argv builders: typed arguments that follow a verb's fixed tokens. Nothing here spawns or
// decides; the owner issues the calls.

// labelArgs renders labels as `--label key=value`, in key order so an argv is reproducible.
func labelArgs(labels map[string]string) []arg {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	out := make([]arg, 0, len(keys)+len(keys))
	for _, key := range keys {
		out = append(out, arg{val: "--label"}, arg{val: key + "=" + labels[key]})
	}

	return out
}

// networkCreateArgs is `network create`'s arguments. The subnet is always the caller's, never the
// engine's pick: an engine-allocated pool can land on a local interface's prefix.
//
//nolint:revive // internal is the network's own property, rendered as its flag, not a control flag.
func networkCreateArgs(name string, labels map[string]string, internal bool, subnet netip.Prefix) []arg {
	out := labelArgs(labels)
	if internal {
		out = append(out, arg{val: "--internal"})
	}

	return append(out, arg{val: "--ipv6=false"}, arg{val: "--subnet"}, arg{val: subnet.String()}, arg{val: name})
}

// volumeCreateArgs is `volume create`'s arguments.
func volumeCreateArgs(name string, labels map[string]string) []arg {
	return append(labelArgs(labels), arg{val: name})
}
