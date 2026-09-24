// Package provision is the one owner of everything Stutter creates on the user's container engine.
//
// The engine it drives also holds the user's own projects, containers named like Stutter's, a
// compose file that may set any label or name, a second check, and a ledger a crash left half
// written. So nothing here treats a name, a label or a filter as permission to remove, mount or
// write. Every call goes through one runner that spawns the docker CLI from a closed table of
// verbs, each under its deadline, in its own process group, and dying with Stutter. Every mutation
// is written to a per-check ledger before it is issued and verified by inspect after it; every
// removal re-verifies the recorded ID and this check's labels first, and nothing is ever pruned.
package provision
