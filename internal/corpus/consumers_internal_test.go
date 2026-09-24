package corpus

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Wintersta7e/stutter/internal/policy"
)

// TestTheMappingReadsTheServersFields: legality is decided from the configuration the server holds,
// field by field, so each field is carried across exactly as read back — a filter written either way
// the server accepts it becomes one sorted set.
func TestTheMappingReadsTheServersFields(t *testing.T) {
	t.Parallel()

	const (
		created  = "orders.created"
		returned = "orders.returned"
		shipped  = "orders.shipped"
	)

	cases := []struct {
		name   string
		server jetstream.ConsumerConfig
		want   policy.Config
	}{
		{
			name: "explicit, server defaults",
			server: jetstream.ConsumerConfig{
				AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second, MaxDeliver: -1, MaxAckPending: 1000,
			},
			want: policy.Config{
				AckMode: policy.AckExplicit, AckWait: 30 * time.Second, MaxDeliver: -1, MaxAckPending: 1000,
			},
		},
		{
			name: "all, a delivery limit",
			server: jetstream.ConsumerConfig{
				AckPolicy: jetstream.AckAllPolicy, AckWait: time.Second, MaxDeliver: 5, MaxAckPending: 10,
			},
			want: policy.Config{AckMode: policy.AckAll, AckWait: time.Second, MaxDeliver: 5, MaxAckPending: 10},
		},
		{
			name:   "none",
			server: jetstream.ConsumerConfig{AckPolicy: jetstream.AckNonePolicy, MaxDeliver: -1},
			want:   policy.Config{AckMode: policy.AckNone, MaxDeliver: -1},
		},
		{
			name: "a backoff curve",
			server: jetstream.ConsumerConfig{
				AckWait: time.Second, BackOff: []time.Duration{time.Second, 5 * time.Second}, MaxDeliver: -1,
			},
			want: policy.Config{
				AckMode: policy.AckExplicit, AckWait: time.Second,
				BackOff: []time.Duration{time.Second, 5 * time.Second}, MaxDeliver: -1,
			},
		},
		{
			name: "one filter and several, overlapping",
			server: jetstream.ConsumerConfig{
				FilterSubject: shipped, FilterSubjects: []string{returned, created, shipped},
			},
			want: policy.Config{AckMode: policy.AckExplicit, FilterSubjects: []string{created, returned, shipped}},
		},
	}

	t.Logf("%d mappings checked", len(cases))

	if len(cases) == 0 {
		t.Fatal("no mappings to check")
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, err := mapConfig(testCase.server)
			if err != nil {
				t.Fatalf("mapConfig() error = %v", err)
			}

			if !reflect.DeepEqual(got, testCase.want) {
				t.Errorf("mapConfig() = %+v, want %+v", got, testCase.want)
			}
		})
	}
}

// TestAnUnmappableAckPolicyIsRefused: an acknowledgement policy the legality table has no row for
// licenses nothing, and guessing one would license faults the bus cannot commit.
func TestAnUnmappableAckPolicyIsRefused(t *testing.T) {
	t.Parallel()

	refused := []jetstream.AckPolicy{jetstream.AckFlowControlPolicy}

	t.Logf("%d unmappable policies checked", len(refused))

	if len(refused) == 0 {
		t.Fatal("no policies to check")
	}

	for _, ackPolicy := range refused {
		_, err := mapConfig(jetstream.ConsumerConfig{AckPolicy: ackPolicy})
		if !errors.Is(err, ErrUnmappable) {
			t.Fatalf("mapConfig(%s) error = %v, want ErrUnmappable", ackPolicy, err)
		}

		for _, want := range []string{"AckPolicy", ackPolicy.String()} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("mapConfig(%s) error = %q, want it to name %q", ackPolicy, err, want)
			}
		}
	}
}
