package compose

import (
	"errors"
	"fmt"
	"strconv"
)

var (
	// ErrUnsupportedCompose means no compose plugin was found, or one below MinVersion.
	ErrUnsupportedCompose = errors.New("unsupported compose")
	// ErrModel means the compose model cannot be used as given: the parse failed, the service is
	// absent, the bus or a name is ambiguous, a datastore lies outside the model, a service is
	// attached to the target, or the `x-stutter` declaration is wrong.
	ErrModel = errors.New("compose model")
	// ErrRefused means a compose key asks for something Stutter will not run. The error is a
	// *Refusal naming the service, the key and its class.
	ErrRefused = errors.New("refused compose key")
	// ErrAudit means a created container differs from the spec it was created from.
	ErrAudit = errors.New("created container differs from its spec")
	// ErrEncryptedEndpoint means a started dependency is reached over TLS on a port Stutter can
	// only compare as bytes, which no two runs could ever agree on.
	ErrEncryptedEndpoint = errors.New("encrypted dependency endpoint")
	// ErrHandshakeContradiction means a port classified as Postgres did not answer a Postgres
	// handshake as Postgres does.
	ErrHandshakeContradiction = fmt.Errorf("%w: handshake contradiction", ErrModel)
)

// Class is the reason a compose key is refused.
type Class uint8

const (
	// K1 is `privileged: true`.
	K1 Class = iota + 1
	// K2 is a refused capability in `cap_add`.
	K2
	// K3 is a `network_mode` that leaves Stutter's network.
	K3
	// K4 is a host or shared namespace.
	K4
	// K5 is a device.
	K5
	// K6 is a runtime or isolation choice.
	K6
	// K7 is access to another container, the engine, or a provider.
	K7
	// K8 is a lifecycle hook.
	K8
	// K9 is a mount type or option Stutter cannot make safe.
	K9
	// K10 is a bind source that is missing or not a regular file or directory.
	K10
	// K11 is a copied-in file whose target is read-only or hidden, or an external config or
	// secret.
	K11
	// K12 is a label in Stutter's own namespace.
	K12
	// K13 is a mount or copy at or above the CA's mount path.
	K13
	// K14 is a key the verdict table does not list.
	K14
)

// String returns the class id, `K1` … `K14`.
func (c Class) String() string {
	return "K" + strconv.Itoa(int(c))
}

// Refusal is a compose key Stutter refuses to run. It unwraps to ErrRefused.
//
//nolint:errname // a refusal is a verdict on the user's model; callers match its class, not its suffix.
type Refusal struct {
	// Service is the service carrying the key; empty for a top-level key.
	Service string
	// Key is the key path, e.g. `cap_add` or `volumes.bind.selinux`.
	Key string
	// Class is the refusal's class.
	Class Class
}

// Error names the service, the key path — for K10, the host path — and the class; never a value.
func (r *Refusal) Error() string {
	what := "key"
	if r.Class == K10 {
		what = "bind source"
	}

	if r.Service == "" {
		if r.Class != K10 {
			what = "top-level key"
		}

		return fmt.Sprintf("compose %s %s is refused (%s)", what, r.Key, r.Class)
	}

	return fmt.Sprintf("compose service %s: %s %s is refused (%s)", r.Service, what, r.Key, r.Class)
}

// Unwrap returns ErrRefused.
func (*Refusal) Unwrap() error {
	return ErrRefused
}
