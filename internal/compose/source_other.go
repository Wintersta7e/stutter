//go:build !linux

package compose

// pseudoFilesystem reports no kernel pseudo-filesystem off Linux: a compose check refuses to run
// there before any source is checked.
func pseudoFilesystem(string) bool {
	return false
}
