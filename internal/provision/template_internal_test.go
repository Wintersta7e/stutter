package provision

import (
	"bytes"
	"encoding/json"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"text/template"
)

// executeAsTheCLI runs an inspect template the way the docker CLI runs one that names .Id, a field
// its typed inspect structs lack: over the engine's JSON decoded into maps, with missingkey=error,
// so a key the JSON omits fails the whole call.
func executeAsTheCLI(t *testing.T, tmpl, body string) (string, error) {
	t.Helper()

	parsed, err := template.New("").Funcs(template.FuncMap{"json": cliJSON}).Parse(tmpl)
	if err != nil {
		t.Fatalf("parse the template: %v", err)
	}

	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.UseNumber()

	var raw any
	if err := decoder.Decode(&raw); err != nil {
		t.Fatalf("decode the inspect body: %v", err)
	}

	var out strings.Builder
	if err := parsed.Option("missingkey=error").Execute(&out, raw); err != nil {
		return "", err
	}

	return out.String(), nil
}

// cliJSON is the docker CLI's json template function.
func cliJSON(v any) (string, error) {
	var buf bytes.Buffer

	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(v); err != nil {
		return "", err
	}

	return strings.TrimSpace(buf.String()), nil
}

// A network's IPAM entry reports a gateway only when the engine recorded one, and its subnet key is
// optional too. Measured: a 28.0.4 engine records no gateway for a network created with --subnet
// alone, and 29.6.2 records one; the read must succeed on every shape, and an absent key reads as
// nothing, never as a value.
func TestTheNetworkReadSurvivesAnEngineThatRecordsNoGateway(t *testing.T) {
	t.Parallel()

	const noGateway = `{"Name":"stutter-0123-service","Id":"6bcb8a71","Created":"2026-09-24T18:34:15Z",` +
		`"Scope":"local","Driver":"bridge","EnableIPv4":true,"EnableIPv6":false,` +
		`"IPAM":{"Driver":"default","Options":{},"Config":[{"Subnet":"10.231.8.0/24"}]},` +
		`"Internal":true,"Attachable":false,"Ingress":false,"ConfigFrom":{"Network":""},` +
		`"ConfigOnly":false,"Containers":{},"Options":{},"Labels":{"example":"1"}}`

	withGateway := strings.Replace(noGateway, `{"Subnet":"10.231.8.0/24"}`,
		`{"Subnet":"10.231.8.0/24","Gateway":"10.231.8.1"}`, 1)
	noSubnet := strings.Replace(noGateway, `{"Subnet":"10.231.8.0/24"}`, `{"Gateway":"10.231.8.1"}`, 1)
	subnet := netip.MustParsePrefix("10.231.8.0/24")

	cases := []struct {
		gateway    netip.Addr
		name, body string
		subnets    []netip.Prefix
	}{
		{name: "no gateway recorded", body: noGateway, subnets: []netip.Prefix{subnet}},
		{
			name: "gateway recorded", body: withGateway, subnets: []netip.Prefix{subnet},
			gateway: netip.MustParseAddr("10.231.8.1"),
		},
		{
			name: "no subnet recorded", body: noSubnet, subnets: []netip.Prefix{{}},
			gateway: netip.MustParseAddr("10.231.8.1"),
		},
	}

	for _, c := range cases {
		out, err := executeAsTheCLI(t, networkTemplate, c.body)
		if err != nil {
			t.Errorf("%s: the network template fails: %v", c.name, err)

			continue
		}

		var report networkReport
		if err := json.Unmarshal([]byte(out), &report); err != nil {
			t.Errorf("%s: the template printed %q: %v", c.name, out, err)

			continue
		}

		if !slices.Equal(report.ipv4Subnets(), c.subnets) || !report.Internal || report.IPv6 ||
			report.Labels["example"] != "1" {
			t.Errorf("%s: read %+v", c.name, report)
		}

		var gateway netip.Addr

		for _, text := range report.Gateways {
			if addr, err := netip.ParseAddr(text); err == nil {
				gateway = addr
			}
		}

		if gateway != c.gateway {
			t.Errorf("%s: gateway %v, want %v", c.name, gateway, c.gateway)
		}
	}
}
