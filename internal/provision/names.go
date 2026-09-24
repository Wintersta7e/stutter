package provision

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/Wintersta7e/stutter/internal/provision/rules"
	"github.com/Wintersta7e/stutter/internal/version"
)

// checkIDBytes is the check ID's length in random bytes: 128 bits, derived from nothing the user
// controls.
const checkIDBytes = 16

// newCheckID mints a check ID: 32 lowercase hex digits.
func newCheckID() (string, error) {
	var raw [checkIDBytes]byte

	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("mint a check ID: %w", err)
	}

	return hex.EncodeToString(raw[:]), nil
}

// isCheckID reports whether text is a check ID: exactly 32 lowercase hex digits.
func isCheckID(text string) bool {
	if len(text) != 2*checkIDBytes {
		return false
	}

	for _, r := range text {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}

	return true
}

// Engine names are for humans and for recovering an interrupted create; never an authority. No name
// carries user text, and seq is the ledger sequence number of the intent that created the resource.

// containerName names a container. The target, the probe start and discovery keep one name for the
// whole check — the engine's reverse DNS tells a container its own name, and a per-run name would
// put a per-run value into a service that logs it. Every other container carries its seq.
func containerName(id string, kind rules.Kind, seq int) string {
	if kind == rules.KindTarget || kind == rules.KindProbe || kind == rules.KindDiscovery {
		return "stutter-" + id + "-" + string(kind)
	}

	return "stutter-" + id + "-" + string(kind) + "-" + strconv.Itoa(seq)
}

// networkName names a network by its role.
func networkName(id, role string) string {
	return "stutter-" + id + "-network-" + role
}

// volumeName names a named volume.
func volumeName(id string, kind rules.Kind, seq int) string {
	return "stutter-" + id + "-" + string(kind) + "-" + strconv.Itoa(seq)
}

// imageRef is an image reference Stutter reserves: under the reserved domain, scoped to the check,
// tagged with the intent's seq so it is always tag-legal and unique in the check.
func imageRef(id string, kind rules.Kind, seq int) string {
	return rules.ImageDomain + "/" + id + "/" + string(kind) + ":" + strconv.Itoa(seq)
}

// project is the compose project name of a check's builds.
func project(id string) string {
	return "stutter-" + id
}

// labelSet is what every resource the check creates carries. The service label is omitted when the
// resource serves no compose service.
func labelSet(id string, kind rules.Kind, service string) map[string]string {
	labels := map[string]string{
		rules.LabelCheck: id,
		rules.LabelKind:  string(kind),
		rules.LabelBuild: version.String(),
	}

	if service != "" {
		labels[rules.LabelService] = service
	}

	return labels
}
