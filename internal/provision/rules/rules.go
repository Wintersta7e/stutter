// Package rules holds the identity every resource Stutter creates on a container engine carries:
// the label namespace and its keys, the kind vocabulary, and the image domain.
//
// It imports nothing internal, so the driver and the verification suite share one declaration of
// these values without either importing the other.
package rules

// Namespace is the label namespace of every resource Stutter creates.
const Namespace = "com.github.wintersta7e.stutter"

// Label keys Stutter writes on every container, network, volume and image it creates. The check
// and kind keys are read back on every verification; the build and service keys are for diagnosis
// only. A `.test` key is reserved for the verification suite and is never declared or written here.
const (
	// LabelCheck holds the check ID.
	LabelCheck = Namespace + ".check"
	// LabelKind holds the resource's Kind.
	LabelKind = Namespace + ".kind"
	// LabelBuild holds Stutter's build identity.
	LabelBuild = Namespace + ".build"
	// LabelService holds the compose service the resource serves; absent when it serves none.
	LabelService = Namespace + ".service"
)

// ImageDomain is the reserved domain every image reference Stutter creates lives under. A reserved
// top-level domain can never be pulled, pushed, or collide with a registry.
const ImageDomain = "stutter.invalid"

// Kind is what a resource is to a check: the value of LabelKind.
type Kind string

// The kind vocabulary. Kinds returns every one of them.
const (
	// KindTarget is every run's target container, its build, and its per-run volumes.
	KindTarget Kind = "target"
	// KindProbe is the probe start's container and its volumes.
	KindProbe Kind = "probe"
	// KindDiscovery is the discovery start's container and its volumes.
	KindDiscovery Kind = "discovery"
	// KindRelay is every relay container and the relay image.
	KindRelay Kind = "relay"
	// KindDependency is a dependency's build.
	KindDependency Kind = "dependency"
	// KindJob is every job container, a job's build, and its volumes.
	KindJob Kind = "job"
	// KindSeed is a dependency's seed container and its volumes.
	KindSeed Kind = "seed"
	// KindSnapshot is every committed image.
	KindSnapshot Kind = "snapshot"
	// KindRestore is every per-run restore container and its per-run volumes.
	KindRestore Kind = "restore"
	// KindTemplateVolume is a template volume.
	KindTemplateVolume Kind = "template-volume"
	// KindVerifier is a verification or classification container and its volumes.
	KindVerifier Kind = "verifier"
	// KindHelper is a volume-copy helper container.
	KindHelper Kind = "helper"
	// KindNetwork is every network.
	KindNetwork Kind = "network"
)

// Kinds returns the whole kind vocabulary, in declaration order.
func Kinds() []Kind {
	return []Kind{
		KindTarget, KindProbe, KindDiscovery, KindRelay, KindDependency, KindJob, KindSeed,
		KindSnapshot, KindRestore, KindTemplateVolume, KindVerifier, KindHelper, KindNetwork,
	}
}
