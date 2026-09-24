// Package dependencytest holds the dependency provisioning tests that run against the real engine:
// classification, seeding, jobs, snapshots, restores and liveness. It has no production code; its
// tests live in their own package directory so the slow Docker suite never evicts the unit tests.
package dependencytest
