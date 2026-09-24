package corpus

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrOwnership means the bound stream cannot be filled and checked as it is, or could not be made:
// it does not capture the corpus, it mirrors, republishes or transforms what it stores, it stores
// without acknowledging, or the service turned out to consume from a different stream.
var ErrOwnership = errors.New("the stream cannot hold the corpus for a check")

// Survey is every stream on the bus, and the bound stream's configuration when it exists.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Survey struct {
	// Streams maps every stream's name to the subjects it captures.
	Streams map[string][]string
	// bound is the bound stream's configuration as the server holds it; nil when it does not exist.
	bound *jetstream.StreamConfig
}

// Exists reports whether the bound stream existed when the survey was taken.
func (s Survey) Exists() bool {
	return s.bound != nil
}

// Survey reads every stream on the bus and the bound stream's configuration.
func (c *Corpus) Survey(ctx context.Context) (Survey, error) {
	surveyed := Survey{Streams: make(map[string][]string)}

	lister := c.stream.ListStreams(ctx)
	for info := range lister.Info() {
		surveyed.Streams[info.Config.Name] = slices.Clone(info.Config.Subjects)

		if info.Config.Name == c.topic.Stream {
			config := info.Config
			surveyed.bound = &config
		}
	}

	if err := lister.Err(); err != nil {
		return Survey{}, fmt.Errorf("survey the bus's streams: %w", err)
	}

	return surveyed, nil
}

// Establish settles who owns the bound stream from a survey taken before the service first started
// and one taken after, validates it, and makes it.
//
// Whoever creates the stream first in production owns it: a job (it existed before), then the service
// (it existed only after the service's first start), then Stutter. A job's stream is left exactly as
// the job made it. The service's is created with the configuration it read back as — so the service's
// own create on every later start finds it unchanged — except a memory-storage one, which the service
// recreates itself on every start. Stutter's captures exactly the corpus's subjects.
//
// Everything is validated before anything is created. The caller restores the checkpoint taken
// before the first survey first, so the stream a service created is not there when this runs.
func (c *Corpus) Establish(ctx context.Context, before, after Survey, messages []Message) error {
	subjects := corpusSubjects(messages)

	var config jetstream.StreamConfig

	switch {
	case before.Exists():
		config = *before.bound
	case after.Exists():
		config = *after.bound
	default:
		config = streamConfig(c.topic.Stream, subjects)
	}

	if err := validate(config, subjects); err != nil {
		return err
	}

	if err := elsewhere(before, after, c.topic.Stream, subjects); err != nil {
		return err
	}

	c.topic.Subjects = slices.Clone(config.Subjects)

	if before.Exists() || config.Storage == jetstream.MemoryStorage {
		return nil
	}

	if _, err := c.stream.CreateStream(ctx, config); err != nil {
		return fmt.Errorf("%w: create stream %s: %w", ErrOwnership, config.Name, err)
	}

	return nil
}

// validate refuses a stream configuration that cannot be filled and checked.
//
// Fill reads an acknowledgement for every message and expects each to land, as published, in this
// stream: a stream that stores without acknowledging, copies from elsewhere, republishes or rewrites
// subjects breaks one of those, and a subject the stream does not capture never lands at all.
func validate(config jetstream.StreamConfig, subjects []string) error {
	for _, setting := range []struct {
		key string
		set bool
	}{
		{key: "Mirror", set: config.Mirror != nil},
		{key: "RePublish", set: config.RePublish != nil},
		{key: "SubjectTransform", set: config.SubjectTransform != nil},
		{key: "NoAck", set: config.NoAck},
	} {
		if setting.set {
			return fmt.Errorf("%w: stream %s sets %s", ErrOwnership, config.Name, setting.key)
		}
	}

	captures := config.Subjects
	if len(captures) == 0 {
		// A stream with no subjects captures its own name.
		captures = []string{config.Name}
	}

	for _, subject := range subjects {
		if !captured(subject, captures) {
			return fmt.Errorf("%w: stream %s does not capture corpus subject %s (it captures %s)",
				ErrOwnership, config.Name, subject, strings.Join(captures, ", "))
		}
	}

	return nil
}

// elsewhere refuses a stream the service created, other than the bound one, that captures a corpus
// subject: the service consumes from that stream, not the bound one.
func elsewhere(before, after Survey, bound string, subjects []string) error {
	var created []string

	for name := range after.Streams {
		if _, existed := before.Streams[name]; !existed && name != bound {
			created = append(created, name)
		}
	}

	slices.Sort(created)

	for _, name := range created {
		for _, subject := range subjects {
			if captured(subject, after.Streams[name]) {
				return fmt.Errorf("%w: the service created stream %s, which captures corpus subject %s; "+
					"it most likely consumes from %s rather than %s", ErrOwnership, name, subject, name, bound)
			}
		}
	}

	return nil
}

// captured reports whether any of a stream's subject filters matches subject.
func captured(subject string, filters []string) bool {
	return slices.ContainsFunc(filters, func(filter string) bool {
		return server.SubjectMatchesFilter(subject, filter)
	})
}

// corpusSubjects are the corpus's distinct subjects, sorted.
func corpusSubjects(messages []Message) []string {
	subjects := make([]string, 0, len(messages))
	for _, message := range messages {
		subjects = append(subjects, message.Subject)
	}

	slices.Sort(subjects)

	return slices.Compact(subjects)
}
