// Package topologydocker holds the topology package's tests that need a container engine: the relay
// image, the networks, the host-address verification and the relays, read back from the engine. It has
// no production code; its tests live in their own package directory so that the slow engine-backed
// suite never evicts the topology unit tests.
package topologydocker
