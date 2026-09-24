package nats

// Delivery is one message the bus handed to the service under test.
//
// It exists for the service Stutter provisions rather than drives. A driven handler is called by the
// driver, which opens the attribution window around the call; a containerised consumer pulls for
// itself, and the only place the moment of delivery is visible is here, on the wire.
type Delivery struct {
	// Subject is the subject the message was published to.
	Subject string
	// Payload is the message body, headers excluded.
	Payload []byte
	// Ack identifies what is being delivered — stream, consumer, sequence and attempt — recovered
	// from the reply subject the consumer will acknowledge on.
	Ack Ack
}

// Deliveries is notified of each message the bus hands to the service under test.
//
// Notification happens BEFORE the bytes are forwarded, so a window opened here is already open when
// the service acts on the message. The other order loses the first effect of every delivery.
//
// It never swallows or reorders a delivery. A pull consumer counts a swallowed message against the
// batch it asked for, so the service's own fetch comes back short and then stalls: scoping belongs in
// what the stream contains. A Hold only delays: while the corpus is published every delivery waits,
// is noted here as it is read, and is forwarded in order on release — well within the consumer's own
// timers, measured every run, and a hold past its bound stops the run.
type Deliveries interface {
	Delivered(delivery Delivery)
}
