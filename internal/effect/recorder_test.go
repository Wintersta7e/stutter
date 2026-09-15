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
