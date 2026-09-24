package topology

import (
	"net/netip"
	"slices"

	"github.com/Wintersta7e/stutter/internal/harness"
)

// detectMode decides how containers reach the host's listeners. When the dependency network's gateway
// is an address of the host's own, they reach it there: gateway mode. Otherwise the engine runs
// elsewhere — Docker Desktop's VM — and they reach the host through the address it gives
// host.docker.internal: host-alias mode.
func detectMode(gateway netip.Addr, local []netip.Addr) harness.Mode {
	if slices.Contains(local, gateway) {
		return harness.ModeGateway
	}

	return harness.ModeHostAlias
}
