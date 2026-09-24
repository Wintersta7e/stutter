// Package composedocker holds the compose package's tests that need a container engine: the
// Postgres handshake against real server images. It has no production code; its tests live in their
// own package directory so that the slow engine-backed suite never evicts the compose unit tests.
package composedocker
