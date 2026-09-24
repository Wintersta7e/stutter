package http_test

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	proxyhttp "github.com/Wintersta7e/stutter/internal/proxy/http"
)

// routesExample is the routes file the specification shows.
const routesExample = `{ "default": {"status": 200, "json": {}},
  "routes": [
    {"method": "GET", "host": "api.example.test", "path": "/v1/accounts/42", "status": 200,
     "json": {"active": true}},
    {"host": "hooks.example.test:8080", "path": "/notify", "status": 204, "headers": {"X-Seen": "1"}} ] }`

// TestDecodeRoutes: a routes file is the Go configuration written down, never a second model, and a
// file that says something the stub would not do is refused, naming where in the file it says it.
func TestDecodeRoutes(t *testing.T) {
	t.Parallel()

	t.Run("the example", func(t *testing.T) {
		t.Parallel()

		fallback, routes, err := proxyhttp.DecodeRoutes(strings.NewReader(routesExample))
		if err != nil {
			t.Fatalf("DecodeRoutes() error = %v", err)
		}

		jsonType := http.Header{"Content-Type": {"application/json"}}
		wantDefault := proxyhttp.Response{Header: jsonType, Body: []byte("{}"), StatusCode: http.StatusOK}
		wantRoutes := []proxyhttp.Route{
			{
				Method: "GET",
				Host:   "api.example.test",
				Path:   "/v1/accounts/42",
				Response: proxyhttp.Response{
					Header:     jsonType,
					Body:       []byte(`{"active":true}`),
					StatusCode: http.StatusOK,
				},
			},
			{
				Host: "hooks.example.test:8080",
				Path: "/notify",
				Response: proxyhttp.Response{
					Header:     http.Header{"X-Seen": {"1"}},
					StatusCode: http.StatusNoContent,
				},
			},
		}

		if !reflect.DeepEqual(fallback, wantDefault) {
			t.Errorf("default = %+v, want %+v", fallback, wantDefault)
		}

		if !reflect.DeepEqual(routes, wantRoutes) {
			t.Errorf("routes = %+v, want %+v", routes, wantRoutes)
		}
	})

	t.Run("an empty object is the zero default", func(t *testing.T) {
		t.Parallel()

		fallback, routes, err := proxyhttp.DecodeRoutes(strings.NewReader("{}"))
		if err != nil || !reflect.DeepEqual(fallback, proxyhttp.Response{}) || routes != nil {
			t.Errorf("DecodeRoutes({}) = %+v, %+v, %v, want the zero default and no routes", fallback, routes, err)
		}
	})

	t.Run("a named content type is kept", func(t *testing.T) {
		t.Parallel()

		fallback, _, err := proxyhttp.DecodeRoutes(strings.NewReader(
			`{"default": {"json": [2, 1], "headers": {"content-type": "application/problem+json"}}}`))
		if err != nil {
			t.Fatalf("DecodeRoutes() error = %v", err)
		}

		want := http.Header{"Content-Type": {"application/problem+json"}}
		if !reflect.DeepEqual(fallback.Header, want) || string(fallback.Body) != "[2,1]" {
			t.Errorf("default = %+v, want only the named content type and the body in file order", fallback)
		}
	})

	checked := 0

	for _, testCase := range routeErrors() {
		checked++

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := proxyhttp.DecodeRoutes(strings.NewReader(testCase.file))
			if err == nil || !strings.Contains(err.Error(), testCase.path+":") {
				t.Errorf("DecodeRoutes(%s) error = %v, want one naming %s", testCase.file, err, testCase.path)
			}
		})
	}

	t.Logf("error classes checked: %d", checked)

	if checked == 0 {
		t.Fatal("error classes checked: 0")
	}
}

type routeError struct {
	name, file, path string
}

func routeErrors() []routeError {
	const status = "default.status"

	return []routeError{
		{name: "unknown key at the top", file: `{"fallback": {}}`, path: "fallback"},
		{name: "unknown key in a route", file: `{"routes": [{"path": "/", "jsn": {}}]}`, path: "routes[0].jsn"},
		{name: "unknown key in a response", file: `{"default": {"jsn": {}}}`, path: "default.jsn"},
		{
			name: "json with text",
			file: `{"routes": [{"path": "/a"}, {"path": "/b", "json": 1, "text": "one"}]}`,
			path: "routes[1]",
		},
		{name: "route without a path", file: `{"routes": [{"host": "api.example.test"}]}`, path: "routes[0].path"},
		{name: "status below 100", file: `{"default": {"status": 99}}`, path: status},
		{name: "status above 599", file: `{"default": {"status": 600}}`, path: status},
		{name: "fractional status", file: `{"default": {"status": 200.5}}`, path: status},
		{name: "quoted status", file: `{"default": {"status": "200"}}`, path: status},
		{
			name: "non-string header value",
			file: `{"routes": [{"path": "/", "headers": {"X-Seen": 1}}]}`,
			path: "routes[0].headers.X-Seen",
		},
		{name: "routes not an array", file: `{"routes": {}}`, path: "routes"},
		{name: "route not an object", file: `{"routes": ["/"]}`, path: "routes[0]"},
		{name: "data after the object", file: `{} {}`, path: "$"},
		{name: "invalid JSON", file: `{"routes": [}`, path: "$"},
		{name: "not an object", file: `[]`, path: "$"},
	}
}
