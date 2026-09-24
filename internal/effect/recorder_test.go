package effect_test

import (
	"testing"

	"github.com/Wintersta7e/stutter/internal/effect"
)

func TestRecorderPreservesStubMetadata(t *testing.T) {
	t.Parallel()

	const requestID = "3f2504e0-4f89-11d3-9a0c-0305e82c3301"

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), []byte("test hash key"))
	recorder.Open("orders", 7, []byte(`{"request_id":"`+requestID+`"}`))

	raw := "POST api.example.test/check body={\"request_id\":\"" + requestID + "\"}"
	canonical := recorder.Canonicalise(raw)
	recorder.Record(effect.Observation{
		Raw:       raw,
		Printable: raw,
		Kind:      effect.KindHTTP,
		Stubbed:   true,
		OffScript: true,
	})

	effects := recorder.Effects()
	if len(effects) != 1 {
		t.Fatalf("Effects() length = %d, want 1", len(effects))
	}

	got := effects[0]
	if got.Canonical != canonical {
		t.Errorf("Canonical = %q, precomputed key was %q", got.Canonical, canonical)
	}

	if !got.Stubbed {
		t.Error("Stubbed = false, want true")
	}

	if !got.OffScript {
		t.Error("OffScript = false, want true")
	}
}

func TestRecorderStillExcludesStubbedSetupTraffic(t *testing.T) {
	t.Parallel()

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), []byte("test hash key"))
	recorder.Record(effect.Observation{
		Raw:       "GET api.example.test/ready",
		Printable: "GET api.example.test/ready",
		Kind:      effect.KindHTTP,
		Stubbed:   true,
	})

	if got := len(recorder.Effects()); got != 0 {
		t.Errorf("Effects() length = %d, want 0", got)
	}

	if got := recorder.SetupCount(); got != 1 {
		t.Errorf("SetupCount() = %d, want 1", got)
	}
}

// TestARefusalBeforeTheFirstDeliveryIsKept: a service whose startup request the bus refused never
// gets as far as consuming, and the refusal is the only record of why. After the first delivery a
// refusal is an effect's Rejected mark, and is not kept a second time.
func TestARefusalBeforeTheFirstDeliveryIsKept(t *testing.T) {
	t.Parallel()

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), []byte("test hash key"))

	startup := effect.Refusal{
		Subject:     "$JS.API.STREAM.CREATE.ORDERS",
		Description: "stream name already in use",
		Code:        400,
		ErrCode:     10058,
	}

	recorder.Declined(startup)
	recorder.Open("orders", 1, []byte(`{}`))
	recorder.Declined(effect.Refusal{Subject: "$JS.API.STREAM.CREATE.LATER", Code: 400, ErrCode: 10058})

	got := recorder.Refusals()
	if len(got) != 1 || got[0] != startup {
		t.Errorf("Refusals() = %+v, want only the one before the first delivery", got)
	}
}

// TestBusCountsAreKeptAcrossTheRun: requests nothing answered and clients that hung up after the
// greeting are counted whenever they happen, before the first delivery or after it.
func TestBusCountsAreKeptAcrossTheRun(t *testing.T) {
	t.Parallel()

	recorder := effect.NewRecorder(effect.NewCanonicaliser(), []byte("test hash key"))

	recorder.NoResponder()
	recorder.ClosedAfterInfo()
	recorder.Open("orders", 1, []byte(`{}`))
	recorder.NoResponder()
	recorder.ClosedAfterInfo()

	if got := recorder.NoResponders(); got != 2 {
		t.Errorf("NoResponders() = %d, want 2", got)
	}

	if got := recorder.ClosedAfterInfoCount(); got != 2 {
		t.Errorf("ClosedAfterInfoCount() = %d, want 2", got)
	}
}
