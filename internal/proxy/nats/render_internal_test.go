package nats

import (
	"strings"
	"testing"
)

// renderWire frames a publish, reads it back and renders it, so every case here goes through the
// same path a real client's bytes take.
func renderWire(t *testing.T, wire string) string {
	t.Helper()

	current := readOne(t, wire)
	headers := parseHeaders(current.body[:current.args.headerLen])

	return render(current.args, headers, current.body[current.args.headerLen:])
}

// TestRenderNamesKeyValueOperations is the point of the package. A handler whose only dedupe guard
// is a key/value claim must produce a line a reader can act on without opening Stutter's source, and
// a claim must not be indistinguishable from an ordinary write.
func TestRenderNamesKeyValueOperations(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		headers string
		payload string
		want    string
	}{
		{
			name:    "an atomic claim asks for sequence zero",
			headers: expectedLastSubjectSeqHeader + ": 0" + crlf,
			want:    "kv.create bucket=" + testBucket + " key=" + testKey,
		},
		{
			name:    "a compare-and-set against a live revision is an update",
			headers: expectedLastSubjectSeqHeader + ": 7" + crlf,
			want:    "kv.update bucket=" + testBucket + " key=" + testKey + " revision=7",
		},
		{
			name:    "a write with nothing to compare against is a put",
			headers: "Nats-Msg-Id: ORD-9001" + crlf,
			want:    "kv.put bucket=" + testBucket + " key=" + testKey + " headers=[Nats-Msg-Id=ORD-9001]",
		},
		{
			name:    "a delete marker announces itself",
			headers: kvOperationHeader + ": DEL" + crlf,
			want:    "kv.delete bucket=" + testBucket + " key=" + testKey,
		},
		{
			name:    "a purge marker announces itself",
			headers: kvOperationHeader + ": PURGE" + crlf + "Nats-Rollup: sub" + crlf,
			want:    "kv.purge bucket=" + testBucket + " key=" + testKey + " headers=[Nats-Rollup=sub]",
		},
		{
			name:    "a claim carries the value it stored",
			headers: expectedLastSubjectSeqHeader + ": 0" + crlf,
			payload: testPayload,
			want:    "kv.create bucket=" + testBucket + " key=" + testKey + " payload=" + testPayload,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := renderWire(t, hpubFrame(testKVSub, "", testCase.headers, testCase.payload))
			if got != testCase.want {
				t.Errorf("render() = %q, want %q", got, testCase.want)
			}
		})
	}
}

// TestRenderWithoutHeadersIsAPut covers a bare publish onto a key/value subject: nothing on the wire
// says more than "a value was written", so nothing more is claimed.
func TestRenderWithoutHeadersIsAPut(t *testing.T) {
	t.Parallel()

	got := renderWire(t, pubFrame(testKVSub, ""))
	want := "kv.put bucket=" + testBucket + " key=" + testKey + " payload=" + testPayload

	if got != want {
		t.Errorf("render() = %q, want %q", got, want)
	}
}

func TestRenderOrdinaryPublish(t *testing.T) {
	t.Parallel()

	got := renderWire(t, pubFrame(testSubject, ""))
	want := "publish subject=" + testSubject + " payload=" + testPayload

	if got != want {
		t.Errorf("render() = %q, want %q", got, want)
	}
}

// TestRenderOrdinaryPublishWithHeaders covers the dedupe key a handler sets on a plain publish,
// which the report has to show for the same reason a key/value claim does.
func TestRenderOrdinaryPublishWithHeaders(t *testing.T) {
	t.Parallel()

	got := renderWire(t, hpubFrame(testSubject, "", "Nats-Msg-Id: ORD-9001"+crlf, ""))
	want := "publish subject=" + testSubject + " headers=[Nats-Msg-Id=ORD-9001]"

	if got != want {
		t.Errorf("render() = %q, want %q", got, want)
	}
}

// TestRenderFlattensAGeneratedInbox is what lets the determinism gate pass at all for a handler that
// publishes: every JetStream publish is a request, and its reply subject is a fresh random inbox on
// every call.
func TestRenderFlattensAGeneratedInbox(t *testing.T) {
	t.Parallel()

	first := renderWire(t, pubFrame(testSubject, inboxPrefix+"one"))
	second := renderWire(t, pubFrame(testSubject, inboxPrefix+"two"))

	if first != second {
		t.Errorf("two runs rendered differently:\n%q\n%q", first, second)
	}

	if !strings.Contains(first, "reply="+inboxPlaceholder) {
		t.Errorf("render() = %q, want the inbox flattened", first)
	}
}

// TestRenderKeepsAFixedReplySubject is the other half: a reply to a subject the handler chose is a
// decision the handler made, and flattening it would hide a real divergence.
func TestRenderKeepsAFixedReplySubject(t *testing.T) {
	t.Parallel()

	got := renderWire(t, pubFrame(testSubject, "orders.replies"))
	if !strings.Contains(got, "reply=orders.replies") {
		t.Errorf("render() = %q, want the reply subject kept", got)
	}
}

// TestRenderIsStableAcrossHeaderOrder guards the comparison the gate depends on: a client writes its
// headers in map order, so two runs of the same handler send the same headers in different orders.
func TestRenderIsStableAcrossHeaderOrder(t *testing.T) {
	t.Parallel()

	forward := "Nats-Msg-Id: a" + crlf + "Nats-Expected-Stream: KV_" + testBucket + crlf
	reverse := "Nats-Expected-Stream: KV_" + testBucket + crlf + "Nats-Msg-Id: a" + crlf

	first := renderWire(t, hpubFrame(testKVSub, "", forward, testPayload))
	second := renderWire(t, hpubFrame(testKVSub, "", reverse, testPayload))

	if first != second {
		t.Errorf("header order changed the rendering:\n%q\n%q", first, second)
	}
}

func TestRenderPayload(t *testing.T) {
	t.Parallel()

	// Field order is dictated by govet's fieldalignment check, not by reading order.
	cases := []struct {
		name    string
		want    string
		payload []byte
	}{
		{
			name:    "readable text is kept so provenance can match it",
			payload: []byte(testPayload),
			want:    testPayload,
		},
		{
			name:    "whitespace collapses so one effect stays one line",
			payload: []byte("{\n  \"order_id\": \"ORD-9001\"\n}"),
			want:    `{ "order_id": "ORD-9001" }`,
		},
		{
			name:    "invalid utf-8 becomes hex",
			payload: []byte{0xff, 0xfe, 0x00},
			want:    "0xfffe00",
		},
		{
			name:    "an embedded control byte becomes hex",
			payload: []byte{'a', 0x01, 'b'},
			want:    "0x610162",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := renderPayload(testCase.payload); got != testCase.want {
				t.Errorf("renderPayload() = %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestSplitKVSubject(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		subject string
		bucket  string
		key     string
		ok      bool
	}{
		{name: "plain", subject: testKVSub, bucket: testBucket, key: testKey, ok: true},
		{
			name:    "a key may contain dots",
			subject: kvMarker + "." + testBucket + ".orders.9001",
			bucket:  testBucket,
			key:     "orders.9001",
			ok:      true,
		},
		{
			name:    "a bucket reached through a domain carries an api prefix",
			subject: "$JS.hub.API." + kvMarker + "." + testBucket + "." + testKey,
			bucket:  testBucket,
			key:     testKey,
			ok:      true,
		},
		{name: "not a key/value subject", subject: testSubject},
		{name: "no key", subject: kvMarker + "." + testBucket},
		{name: "no bucket", subject: kvMarker},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, ok := splitKVSubject(testCase.subject)
			if ok != testCase.ok {
				t.Fatalf("splitKVSubject(%q) ok = %v, want %v", testCase.subject, ok, testCase.ok)
			}

			if ok && (got.bucket != testCase.bucket || got.key != testCase.key) {
				t.Errorf("splitKVSubject(%q) = %+v, want bucket %q key %q",
					testCase.subject, got, testCase.bucket, testCase.key)
			}
		})
	}
}
