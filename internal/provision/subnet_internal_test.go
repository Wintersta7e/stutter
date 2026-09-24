package provision

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"testing"
)

// thisHost is the host the plan measured: two engine networks, and loopback and a WSL eth0 on the
// interfaces.
func thisHost() []Occupied {
	return []Occupied{
		{By: "interface lo", Prefix: netip.MustParsePrefix("127.0.0.0/8")},
		{By: "interface lo", Prefix: netip.MustParsePrefix("10.255.255.254/32")},
		{By: "interface eth0", Prefix: netip.MustParsePrefix("172.21.240.0/20")},
		{By: "network bridge", Prefix: netip.MustParsePrefix("172.17.0.0/16")},
		{By: "network compose_default", Prefix: netip.MustParsePrefix("172.18.0.0/16")},
	}
}

func network(by, prefix string) Occupied {
	return Occupied{By: "network " + by, Prefix: netip.MustParsePrefix(prefix)}
}

// TestTheSelectorSkipsLocalAndEngineSubnets picks a subnet the engine could not refuse and that
// shadows nothing: the engine sees its own networks but not the host's interfaces, so a subnet over an
// interface would be created happily and cut the host off from it.
func TestTheSelectorSkipsLocalAndEngineSubnets(t *testing.T) {
	t.Parallel()

	crowded := append(thisHost(),
		network("a", "172.16.0.0/14"), network("b", "172.20.0.0/16"), network("c", "172.21.0.0/17"),
		network("d", "172.21.128.0/18"), network("e", "172.21.192.0/19"), network("f", "172.21.224.0/20"))

	cases := []struct {
		name string
		want string
		held []Occupied
	}{
		{name: "this host", held: thisHost(), want: "172.16.0.0/24"},
		{name: "the first taken", held: append(thisHost(), network("x", "172.16.0.0/24")), want: "172.16.1.0/24"},
		{name: "free only beside the interface", held: crowded, want: "172.22.0.0/24"},
	}

	for _, testCase := range cases {
		pick, err := pickSubnet(testCase.held, nil)
		if err != nil {
			t.Errorf("%s: pickSubnet() error = %v", testCase.name, err)

			continue
		}

		for _, held := range testCase.held {
			if strings.HasPrefix(held.By, "interface") && held.Prefix.Overlaps(pick) {
				t.Errorf("%s: picked %s, which overlaps a local interface", testCase.name, pick)
			}
		}

		if pick.String() != testCase.want {
			t.Errorf("%s: picked %s, want %s", testCase.name, pick, testCase.want)
		}
	}

	if pick, err := pickSubnet(append(thisHost(), network("all", "172.16.0.0/12")), nil); err == nil {
		t.Errorf("every candidate held: picked %s, want an error", pick)
	}
}

// scriptedEngine is the two engine reads the retry loop makes: what is occupied, and a create that
// fails as told.
type scriptedEngine struct {
	failures []error
	held     []Occupied
	created  []netip.Prefix
}

func (s *scriptedEngine) occupied(context.Context) ([]Occupied, error) {
	return s.held, nil
}

func (s *scriptedEngine) create(_ context.Context, subnet netip.Prefix) (*Network, error) {
	s.created = append(s.created, subnet)

	if len(s.failures) == 0 {
		return &Network{name: "net-" + subnet.String()}, nil
	}

	failure := s.failures[0]
	s.failures = s.failures[1:]

	if errors.Is(failure, ErrSubnetTaken) || errors.Is(failure, ErrSubnetOverlap) {
		// The subnet is held now: another network won the race for it.
		s.held = append(s.held, network("racer", subnet.String()))
	}

	return nil, failure
}

// TestAnOverlapRefusalMovesToTheNextFreeSubnet moves on whichever way the race was lost: the engine
// refusing the pool, or — measured on a native Docker 28 engine, where another network's bridge is a
// host interface — the driver's own interface check refusing it first.
func TestAnOverlapRefusalMovesToTheNextFreeSubnet(t *testing.T) {
	t.Parallel()

	for _, lost := range []error{ErrSubnetTaken, ErrSubnetOverlap} {
		engine := &scriptedEngine{held: thisHost(), failures: []error{fmt.Errorf("%w: race", lost)}}

		created, err := createWithSubnet(t.Context(), engine.occupied, engine.create)
		if err != nil {
			t.Fatalf("%v: err = %v, want the next subnet", lost, err)
		}

		if created.name != "net-172.16.1.0/24" || len(engine.created) != 2 {
			t.Errorf("%v: created %s after %v, want 172.16.1.0/24 on the second attempt", lost, created.name,
				engine.created)
		}
	}
}

func TestARefusalWithoutOverlapIsAFailure(t *testing.T) {
	t.Parallel()

	refused := errors.New("engine unreachable")
	engine := &scriptedEngine{held: thisHost(), failures: []error{refused}}

	if _, err := createWithSubnet(t.Context(), engine.occupied, engine.create); !errors.Is(err, refused) {
		t.Errorf("err = %v, want the engine's own failure", err)
	}

	if len(engine.created) != 1 {
		t.Errorf("%d creates, want 1: a failure that is not a taken subnet is final", len(engine.created))
	}
}

func TestSubnetAttemptsAreBounded(t *testing.T) {
	t.Parallel()

	failures := make([]error, subnetAttempts+1)
	for at := range failures {
		failures[at] = fmt.Errorf("%w: race %d", ErrSubnetTaken, at)
	}

	engine := &scriptedEngine{held: thisHost(), failures: failures}

	_, err := createWithSubnet(t.Context(), engine.occupied, engine.create)
	if !errors.Is(err, ErrSubnetTaken) || len(engine.created) != subnetAttempts {
		t.Fatalf("err = %v after %d creates, want a failure after %d", err, len(engine.created), subnetAttempts)
	}

	for _, subnet := range engine.created {
		if !strings.Contains(err.Error(), subnet.String()) {
			t.Errorf("the failure does not name %s: %v", subnet, err)
		}
	}
}
