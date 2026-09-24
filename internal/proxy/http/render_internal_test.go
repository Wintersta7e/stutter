package http

import (
	nethttp "net/http"
	"testing"
)

func TestIgnoredHeaderHonoursEveryConnectionToken(t *testing.T) {
	t.Parallel()

	headers := nethttp.Header{
		"Connection":  {"keep-alive, X-Transient"},
		"X-Transient": {"must not enter the effect"},
	}

	if !ignoredHeader("X-Transient", headers) {
		t.Error("a header nominated by Connection was retained")
	}
}

// TestStableHostTable: the rendered host is the name the service asked for, never an address a bind or
// advertise choice put in its Host header, so no run's addresses reach an effect.
func TestStableHostTable(t *testing.T) {
	t.Parallel()

	const (
		logical  = "Logical.Invalid"
		rendered = "logical.invalid"
	)

	for _, testCase := range []struct {
		host, want string
	}{
		{host: "", want: rendered},
		{host: "10.0.0.5:8080", want: rendered},
		{host: "[::1]:443", want: rendered},
		{host: "::1", want: rendered},
		{host: "localhost:80", want: rendered},
		{host: "LOCALHOST", want: rendered},
		{host: "API.Example.Test", want: "api.example.test"},
		{host: "api.example.test:8080", want: "api.example.test:8080"},
	} {
		if got := stableHost(testCase.host, logical); got != testCase.want {
			t.Errorf("stableHost(%q) = %q, want %q", testCase.host, got, testCase.want)
		}
	}
}
