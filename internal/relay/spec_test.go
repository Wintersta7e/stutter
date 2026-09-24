package relay_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/relay"
)

// testToken is a fixed token, so a table of argv lists can spell it out; hexToken is its argv form.
var (
	testToken = relay.Token(bytes.Repeat([]byte{0xa5}, len(relay.Token{})))
	hexToken  = hex.EncodeToString(testToken[:])
)

// argv splits a command line written as one string, with TOKEN standing for the test token, so a table
// of argv lists reads as the command lines they are.
func argv(line string) []string {
	return strings.Fields(strings.ReplaceAll(line, "TOKEN", hexToken))
}

func pipe(port uint16, upstream string) relay.Listener {
	return relay.Listener{Upstream: netip.MustParseAddrPort(upstream), Port: port}
}

// TestArgsRoundTrip holds the argv's one owner to its word: whatever a relay is told, it reads back
// exactly, for every relay the topology creates and for the verifier and the copy helper.
func TestArgsRoundTrip(t *testing.T) {
	t.Parallel()

	service := netip.MustParsePrefix("172.16.0.0/24")
	dependency := netip.MustParsePrefix("172.16.1.0/24")

	specs := map[string]relay.Spec{
		"dependency relay with two pipes": {
			Listeners: []relay.Listener{pipe(5432, "127.0.0.1:40001"), pipe(5433, "127.0.0.1:40002")},
			Bind:      service,
			Token:     testToken,
		},
		"bus relay": {
			Listeners: []relay.Listener{pipe(4222, "192.168.65.254:40003"), pipe(8222, "192.168.65.254:40004")},
			Bind:      service,
			Token:     testToken,
		},
		"stub relay": {
			Listeners: []relay.Listener{
				pipe(80, "127.0.0.1:40005"),
				pipe(443, "127.0.0.1:40006"),
				{Upstream: netip.MustParseAddrPort("127.0.0.1:40007"), Kind: relay.CatchAll},
			},
			Bind:   service,
			Signal: netip.MustParseAddrPort("127.0.0.1:40008"),
			Token:  testToken,
		},
		"seed bus relay": {
			Listeners: []relay.Listener{pipe(4222, "127.0.0.1:40009"), pipe(8222, "127.0.0.1:40009")},
			Bind:      dependency,
			Token:     testToken,
		},
	}

	for name, spec := range specs {
		parsed, err := relay.Parse(spec.Args())
		if err != nil {
			t.Errorf("%s: Parse(%q) error = %v", name, spec.Args(), err)

			continue
		}

		if !reflect.DeepEqual(parsed, spec) {
			t.Errorf("%s: Parse(Args()) = %+v, want %+v", name, parsed, spec)
		}
	}

	verifies := map[string]relay.Verify{
		"a literal":            {Target: "172.16.1.1", Dial: 3 * time.Second, Token: testToken, Port: 40010},
		"host.docker.internal": {Target: "host.docker.internal", Dial: 3 * time.Second, Token: testToken, Port: 40011},
	}

	for name, verify := range verifies {
		parsed, err := relay.ParseVerify(verify.Args())
		if err != nil || !reflect.DeepEqual(parsed, verify) {
			t.Errorf("verify on %s: ParseVerify(Args()) = %+v, %v; want %+v", name, parsed, err, verify)
		}
	}

	copied := relay.Copy{Src: "/var/lib/postgresql/data", Dst: "/restore"}

	parsed, err := relay.ParseCopy(copied.Args())
	if err != nil || parsed != copied {
		t.Errorf("ParseCopy(Args()) = %+v, %v; want %+v", parsed, err, copied)
	}
}

// TestAHostnameUpstreamIsAParseError keeps name resolution out of the relay: a relay that resolved
// names would answer from the service network's DNS, which the stub relay owns.
//
// Every name is tried twice: as a dependency's name, which resolves nowhere outside the engine, and as
// localhost, which resolves everywhere — so a parser that resolves names is caught here too, not only
// one that rejects whatever fails to resolve.
func TestAHostnameUpstreamIsAParseError(t *testing.T) {
	t.Parallel()

	for _, host := range []string{"db", "localhost"} {
		upstreams := []string{"--pipe 5432=" + host + ":5432", "--catch-all " + host + ":1", "--dns " + host + ":53"}

		for _, upstream := range upstreams {
			if _, err := relay.Parse(argv("serve --token TOKEN --bind 172.16.0.0/24 " + upstream)); err == nil {
				t.Errorf("%s: err = <nil>, want a parse error", upstream)
			}
		}

		if _, err := relay.ParseVerify(
			argv("verify --token TOKEN --target " + host + " --port 40000 --dial 3s"),
		); err == nil {
			t.Errorf("--target %s: err = <nil>, want a parse error", host)
		}
	}
}

// TestParseRefusesMalformedArgv names the flag in every refusal: the provisioner writes these argv
// lists, so a refusal is a bug to be found, and the flag is where to look.
func TestParseRefusesMalformedArgv(t *testing.T) {
	t.Parallel()

	const (
		serve    = "serve --token TOKEN --bind 172.16.0.0/24 "
		verify   = "verify --token TOKEN --target 172.16.1.1 "
		pipeFlag = "pipe"
	)

	cases := []struct {
		parse func([]string) error
		line  string
		flag  string
	}{
		{parse: parseServe, line: "listen", flag: "listen"},
		{parse: parseServe, line: serve + "--wibble 1", flag: "wibble"},
		{parse: parseServe, line: "serve --bind 172.16.0.0/24", flag: "--token"},
		{parse: parseServe, line: "serve --token TOKEN", flag: "--bind"},
		{parse: parseServe, line: "serve --token abcd --bind 172.16.0.0/24", flag: "token"},
		{
			parse: parseServe,
			line:  "serve --token " + strings.ToUpper(hexToken) + " --bind 172.16.0.0/24",
			flag:  "token",
		},
		{parse: parseServe, line: "serve --token TOKEN --bind fd00::/64", flag: "bind"},
		{parse: parseServe, line: "serve --token TOKEN --bind 172.16.0.9/24", flag: "bind"},
		{parse: parseServe, line: serve + "--pipe 0=127.0.0.1:1", flag: pipeFlag},
		{parse: parseServe, line: serve + "--pipe 65536=127.0.0.1:1", flag: pipeFlag},
		{parse: parseServe, line: serve + "--pipe 80=127.0.0.1:0", flag: pipeFlag},
		{parse: parseServe, line: serve + "--pipe 80=[::1]:80", flag: pipeFlag},
		{parse: parseServe, line: serve + "--pipe 80=127.0.0.1:1 --pipe 80=127.0.0.1:2", flag: pipeFlag},
		{parse: parseServe, line: serve + "--pipe 53=127.0.0.1:1 --dns 127.0.0.1:2", flag: pipeFlag},
		{parse: parseServe, line: serve + "--catch-all 127.0.0.1:1 --catch-all 127.0.0.1:2", flag: "catch-all"},
		{parse: parseServe, line: serve + "extra", flag: "extra"},
		{parse: parseVerify, line: "verify --token TOKEN --port 1 --dial 1s", flag: "--target"},
		{parse: parseVerify, line: verify + "--dial 1s", flag: "--port"},
		{parse: parseVerify, line: verify + "--port 1", flag: "--dial"},
		{parse: parseVerify, line: verify + "--port 0 --dial 1s", flag: "port"},
		{parse: parseVerify, line: verify + "--port 1 --dial 0s", flag: "dial"},
		{parse: parseVerify, line: verify + "--port 1 --dial -1s", flag: "dial"},
		{parse: parseCopy, line: "copy --src data --dst /restore", flag: "src"},
		{parse: parseCopy, line: "copy --src /data --dst restore", flag: "dst"},
		{parse: parseCopy, line: "copy --src /data", flag: "--dst"},
	}

	for _, testCase := range cases {
		err := testCase.parse(argv(testCase.line))
		if err == nil {
			t.Errorf("%q parsed, want an error naming %q", testCase.line, testCase.flag)

			continue
		}

		if !strings.Contains(err.Error(), testCase.flag) {
			t.Errorf("%q: error %q does not name %q", testCase.line, err, testCase.flag)
		}
	}

	t.Logf("malformed argv refused: %d", len(cases))
}

func parseServe(args []string) error {
	_, err := relay.Parse(args)

	return err
}

func parseVerify(args []string) error {
	_, err := relay.ParseVerify(args)

	return err
}

func parseCopy(args []string) error {
	_, err := relay.ParseCopy(args)

	return err
}

func TestQueryRecordRoundTrip(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." +
		strings.Repeat("d", 61)
	if len(long) != 253 {
		t.Fatalf("the long name is %d characters, want 253", len(long))
	}

	queries := []relay.Query{
		{Name: "example.test.", Type: 1},
		{Name: "_x._tcp.example.test.", Type: 33},
		{Name: "mail.example.test.", Type: 15},
		{Name: long, Type: 16},
	}

	var stream bytes.Buffer

	for _, query := range queries {
		if err := relay.WriteQuery(&stream, query); err != nil {
			t.Fatalf("WriteQuery(%+v) error = %v", query, err)
		}
	}

	for _, want := range queries {
		got, err := relay.ReadQuery(&stream)
		if err != nil || got != want {
			t.Errorf("ReadQuery() = %+v, %v; want %+v", got, err, want)
		}
	}

	if _, err := relay.ReadQuery(&stream); !errors.Is(err, io.EOF) {
		t.Errorf("ReadQuery() at the end = %v, want EOF", err)
	}

	if err := relay.WriteQuery(io.Discard, relay.Query{Name: strings.Repeat("x", 256), Type: 1}); err == nil {
		t.Error("WriteQuery() of a 256-byte name: err = <nil>, want a refusal")
	}

	var torn bytes.Buffer

	if err := relay.WriteQuery(&torn, relay.Query{Name: "example.test.", Type: 16}); err != nil {
		t.Fatalf("WriteQuery() error = %v", err)
	}

	torn.Truncate(torn.Len() - 1)

	if _, err := relay.ReadQuery(&torn); err == nil || errors.Is(err, io.EOF) {
		t.Errorf("ReadQuery() of a torn record = %v, want an error that is not EOF", err)
	}
}
