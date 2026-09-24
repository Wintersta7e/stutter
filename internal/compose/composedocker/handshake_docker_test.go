package composedocker_test

import (
	"context"
	"crypto/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/dockertest"
	"github.com/Wintersta7e/stutter/internal/proxy/pg"
)

func TestMain(m *testing.M) {
	dockertest.Main(m)
}

// TestHandshakeAgainstRealImages asks three real servers the Postgres handshake over a published
// port: a Postgres server answers as Postgres, and a server-first and a silent protocol do not.
func TestHandshakeAgainstRealImages(t *testing.T) {
	t.Parallel()

	engine := dockertest.Require(t)

	cases := []struct {
		env   map[string]string
		image string
		want  pg.Answer
		wait  time.Duration
		port  uint16
	}{
		// initdb runs before the server listens, and the engine's forwarder accepts meanwhile.
		{
			image: "postgres:18-alpine", port: 5432, want: pg.AnswerPostgres, wait: 60 * time.Second,
			env: map[string]string{"POSTGRES_PASSWORD": rand.Text()},
		},
		{image: "redis:8-alpine", port: 6379, want: pg.AnswerOther, wait: 10 * time.Second},
		{image: "nats:alpine", port: 4222, want: pg.AnswerOther, wait: 10 * time.Second},
	}

	var (
		mu      sync.Mutex
		answers = map[string]pg.Answer{}
		removed []string
	)

	// A parent's cleanup runs once every parallel case has finished, so it reads every answer.
	t.Cleanup(func() {
		got := make([]string, 0, len(cases))
		for _, tc := range cases {
			got = append(got, string(answers[tc.image]))
		}

		t.Logf("images=%d answers=%s removed=%s", len(answers), strings.Join(got, ","), strings.Join(removed, ","))

		if len(answers) < len(cases) || slices.Contains(got, "") {
			t.Errorf("images=%d, want %d answered", len(answers), len(cases))
		}
	})

	for _, tc := range cases {
		t.Run(tc.image, func(t *testing.T) {
			t.Parallel()

			docker := engine.Docker(t)
			id := docker.Create(t, dockertest.CreateSpec{Image: tc.image, Env: tc.env, Publish: []uint16{tc.port}})
			docker.Start(t, id)

			addr := docker.Port(t, id, tc.port)

			ctx, cancel := context.WithTimeout(t.Context(), tc.wait)
			defer cancel()

			start := time.Now()
			answer, err := pg.Handshake(ctx, addr.String())
			elapsed := time.Since(start)

			docker.Remove(t, id)

			if held := docker.Inspect(t, dockertest.ObjectContainer, id); held != nil {
				t.Errorf("the engine still holds container %s after its removal", id)
			}

			if err != nil {
				t.Fatalf("Handshake(%s): %v", addr, err)
			}

			if answer != tc.want {
				t.Errorf("%s answered %q after %v, want %q", tc.image, answer, elapsed, tc.want)
			}

			t.Logf("%s answered %q after %v; removed %s", tc.image, answer, elapsed, id)

			mu.Lock()
			defer mu.Unlock()

			answers[tc.image] = answer

			removed = append(removed, id)
		})
	}
}
