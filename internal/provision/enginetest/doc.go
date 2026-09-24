// Package enginetest holds the driver's tests that run the provision package the way a user's
// machine sees it: in a separate process that can be killed, against the real docker CLI or a shim
// standing in for it. It has no production code; its tests live in their own package directory so
// that the slow, process-level suite never evicts the driver's unit tests.
package enginetest
