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

func TestStableHostIsCaseInsensitive(t *testing.T) {
	t.Parallel()

	if got := stableHost("API.Example.Test", "unused.invalid", "127.0.0.1:1"); got != "api.example.test" {
		t.Errorf("stableHost() = %q, want a lower-case host", got)
	}
}
