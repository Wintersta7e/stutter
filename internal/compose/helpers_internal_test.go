package compose

const (
	// testTarget is the service under test in every model the internal tests parse.
	testTarget = "api"
	// cacheService is a dependency several models share.
	cacheService = "cache"
)
