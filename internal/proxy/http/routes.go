package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	nethttp "net/http"
	"slices"
	"strconv"
	"strings"
)

// The bounds of a status a routes file may give.
const (
	lowestStatus  = 100
	highestStatus = 599
)

// errRoutes means a routes file says something the stub would not do. Every one names where in the
// file it says it.
var errRoutes = errors.New("invalid stub routes")

// DecodeRoutes reads a routes file: one JSON object whose optional "default" is the response for any
// call no route matches, and whose optional "routes" are matched in file order. It is the Go
// configuration written down — the same Response and Route values — and an empty object is the zero
// default, which answers every call 200 {}.
//
// Anything the stub would not do is refused rather than ignored, and the error names the JSON path of
// the element that says it, as routes[1].json: an unknown key, json beside text, a route with no path,
// a status outside 100–599, a header value that is not a string, data after the object.
func DecodeRoutes(r io.Reader) (Response, []Route, error) {
	decoder := json.NewDecoder(r)
	decoder.UseNumber()

	var top map[string]json.RawMessage
	if err := decoder.Decode(&top); err != nil || top == nil {
		return Response{}, nil, routesError("$", "not one JSON object")
	}

	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Response{}, nil, routesError("$", "data after the top-level object")
	}

	if err := knownKeys("", top, "default", "routes"); err != nil {
		return Response{}, nil, err
	}

	var fallback Response

	if raw, set := top["default"]; set {
		fields, err := object("default", raw)
		if err != nil {
			return Response{}, nil, err
		}

		if unknown := knownKeys("default", fields, responseKeys()...); unknown != nil {
			return Response{}, nil, unknown
		}

		if fallback, err = decodeResponse("default", fields); err != nil {
			return Response{}, nil, err
		}
	}

	routes, err := decodeRouteList(top["routes"])
	if err != nil {
		return Response{}, nil, err
	}

	return fallback, routes, nil
}

// responseKeys are the keys a response may carry.
func responseKeys() []string {
	return []string{"status", "headers", "json", "text"}
}

// decodeRouteList reads the routes array; absent is no routes.
func decodeRouteList(raw json.RawMessage) ([]Route, error) {
	if raw == nil {
		return nil, nil
	}

	var elements []json.RawMessage
	if err := json.Unmarshal(raw, &elements); err != nil || elements == nil {
		return nil, routesError("routes", "not an array")
	}

	routes := make([]Route, 0, len(elements))

	for index, element := range elements {
		route, err := decodeRoute("routes["+strconv.Itoa(index)+"]", element)
		if err != nil {
			return nil, err
		}

		routes = append(routes, route)
	}

	return routes, nil
}

// decodeRoute reads one route: a response, plus the path it matches and optionally a method and host.
func decodeRoute(path string, raw json.RawMessage) (Route, error) {
	fields, err := object(path, raw)
	if err != nil {
		return Route{}, err
	}

	if unknown := knownKeys(path, fields, append(responseKeys(), "path", "method", "host")...); unknown != nil {
		return Route{}, unknown
	}

	if _, set := fields["path"]; !set {
		return Route{}, routesError(path+".path", "a route needs a path")
	}

	var route Route

	for _, field := range []struct {
		target *string
		key    string
	}{{&route.Path, "path"}, {&route.Method, "method"}, {&route.Host, "host"}} {
		if value, set := fields[field.key]; set {
			if *field.target, err = text(path+"."+field.key, value); err != nil {
				return Route{}, err
			}
		}
	}

	if route.Response, err = decodeResponse(path, fields); err != nil {
		return Route{}, err
	}

	return route, nil
}

// decodeResponse reads the response keys of fields, the object at path.
func decodeResponse(path string, fields map[string]json.RawMessage) (Response, error) {
	var response Response

	if raw, set := fields["status"]; set {
		status, err := strconv.Atoi(string(raw))
		if err != nil || status < lowestStatus || status > highestStatus {
			return Response{}, routesError(path+".status", "not an integer from 100 to 599")
		}

		response.StatusCode = status
	}

	if raw, set := fields["headers"]; set {
		header, err := decodeHeaders(path+".headers", raw)
		if err != nil {
			return Response{}, err
		}

		response.Header = header
	}

	body, isJSON, err := decodeBody(path, fields)
	if err != nil {
		return Response{}, err
	}

	response.Body = body

	if isJSON && !hasHeader(response.Header, "Content-Type") {
		if response.Header == nil {
			response.Header = nethttp.Header{}
		}

		response.Header.Set("Content-Type", "application/json")
	}

	return response, nil
}

// decodeBody reads a response's body: json written compact in file order, text verbatim, or neither
// and no body. isJSON reports which.
func decodeBody(path string, fields map[string]json.RawMessage) ([]byte, bool, error) {
	structured, hasJSON := fields["json"]
	plain, hasText := fields["text"]

	switch {
	case hasJSON && hasText:
		return nil, false, routesError(path, "json and text are exclusive")
	case hasJSON:
		var compact bytes.Buffer
		if err := json.Compact(&compact, structured); err != nil {
			return nil, false, routesError(path+".json", "not JSON")
		}

		return compact.Bytes(), true, nil
	case hasText:
		value, err := text(path+".text", plain)

		return []byte(value), false, err
	default:
		return nil, false, nil
	}
}

// decodeHeaders reads a headers object: every value one string.
func decodeHeaders(path string, raw json.RawMessage) (nethttp.Header, error) {
	fields, err := object(path, raw)
	if err != nil {
		return nil, err
	}

	header := nethttp.Header{}

	for _, name := range slices.Sorted(maps.Keys(fields)) {
		value, err := text(path+"."+name, fields[name])
		if err != nil {
			return nil, err
		}

		header.Add(name, value)
	}

	return header, nil
}

// object reads the JSON object at path.
func object(path string, raw json.RawMessage) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, routesError(path, "not an object")
	}

	return fields, nil
}

// text reads the JSON string at path.
func text(path string, raw json.RawMessage) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || !bytes.HasPrefix(bytes.TrimSpace(raw), []byte(`"`)) {
		return "", routesError(path, "not a string")
	}

	return value, nil
}

// knownKeys refuses the first key of fields, in name order, that is not one of allowed.
func knownKeys(path string, fields map[string]json.RawMessage, allowed ...string) error {
	for _, key := range slices.Sorted(maps.Keys(fields)) {
		if !slices.Contains(allowed, key) {
			return routesError(strings.TrimPrefix(path+"."+key, "."), "unknown key")
		}
	}

	return nil
}

func routesError(path, reason string) error {
	return fmt.Errorf("%w: %s: %s", errRoutes, path, reason)
}
