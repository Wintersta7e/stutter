package harness

// Endpoint keys: the names a proxy failure is reported under.
const (
	// KeyBus is the bus.
	KeyBus = "bus"
	// KeyHTTP is the HTTP stub.
	KeyHTTP = "http"
	// keyDatabase is the database on the Go-caller path, where it has no service name of its own.
	keyDatabase = "database"
)
