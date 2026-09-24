package compose

import (
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// URL schemes the classification reads by name.
const (
	schemeNATS       = "nats"
	schemePostgres   = "postgres"
	schemePostgresql = "postgresql"
	schemeHTTP       = "http"
	schemeHTTPS      = "https"
)

// ref is one endpoint a configuration value names. Only the host, the port and the scheme leave
// the tokenizer — never userinfo, a path, a query or the rest of the value.
type ref struct {
	// Key is the environment key, or `command[i]`/`entrypoint[i]`, the value came from.
	Key string
	// Scheme is a URL's scheme, lower case, with any `jdbc:` prefix stripped; empty otherwise.
	Scheme string
	// Host is the host alone; a socket's path for a socket.
	Host string
	// List numbers the URL lists of one value from 1: the hosts of one seed list share it. Zero is
	// a reference that is not a URL.
	List int
	// Port is the explicit port; zero when the value names none.
	Port uint16
	// JDBC reports a `jdbc:` URL.
	JDBC bool
	// KeywordPG reports a libpq keyword string or a PGHOST/PGPORT variable.
	KeywordPG bool
	// Socket reports a local socket rather than a network host.
	Socket bool
	// TLS reports a Postgres reference whose sslmode requires encryption.
	TLS bool
}

var (
	// urlStart finds where a URL begins: an optional `jdbc:`, a scheme and `://`.
	urlStart = regexp.MustCompile(`(?:jdbc:)?[A-Za-z][A-Za-z0-9+.-]*://`)
	// namePort is a bare `name:port` token; the name must hold a letter to be a host.
	namePort = regexp.MustCompile(`^([A-Za-z0-9][A-Za-z0-9._-]*):([0-9]{1,5})$`)
	// keywordKey is a libpq keyword.
	keywordKey = regexp.MustCompile(`^[a-z_]+$`)
)

// references finds every endpoint value names. Go's url.Parse returns the wrong host, without an
// error, for a password holding `@` or `/` and for a host list, so the value is read by hand.
func references(key, value string) []ref {
	switch key {
	case "PGHOST":
		return pgHosts(key, value, nil, false)
	case "PGPORT":
		if port, ok := parsePort(strings.TrimSpace(value)); ok {
			return []ref{{Key: key, Port: port, KeywordPG: true}}
		}

		return nil
	}

	if refs, ok := keywordString(key, value); ok {
		return refs
	}

	refs, rest := urlReferences(key, value)

	return append(refs, bareReferences(key, rest)...)
}

// urlReferences reads every URL in value and returns the value with those URLs blanked out.
func urlReferences(key, value string) ([]ref, string) {
	starts := urlStart.FindAllStringIndex(value, -1)
	if starts == nil {
		return nil, value
	}

	var refs []ref

	rest := []byte(value)
	list := 0
	previousEnd := -1

	for index, start := range starts {
		end := urlEnd(value, start[1], starts, index)

		// A URL right after `,` that ends where the previous one began its comma continues its list.
		if previousEnd < 0 || start[0] != previousEnd+1 || value[previousEnd] != ',' {
			list++
		}

		refs = append(refs, readURL(key, value[start[0]:start[1]], value[start[1]:end], list)...)

		for at := start[0]; at < end; at++ {
			rest[at] = ' '
		}

		previousEnd = end
	}

	return refs, string(rest)
}

// urlEnd returns where the URL whose `://` ends at body stops: at whitespace, or where a following
// `,scheme://` of the same seed list begins.
func urlEnd(value string, body int, starts [][]int, index int) int {
	end := len(value)
	if space := strings.IndexAny(value[body:], " \t\r\n"); space >= 0 {
		end = body + space
	}

	if index+1 < len(starts) {
		if next := starts[index+1][0]; next <= end && next > body && value[next-1] == ',' {
			return next - 1
		}
	}

	return end
}

// readURL reads one URL: prefix is its `[jdbc:]scheme://`, body what follows.
func readURL(key, prefix, body string, list int) []ref {
	scheme := strings.ToLower(strings.TrimSuffix(prefix, "://"))
	jdbc := strings.HasPrefix(scheme, "jdbc:")
	scheme = strings.TrimPrefix(scheme, "jdbc:")

	// Userinfo runs to the last `@` and is discarded unread; it may hold `@`, `:`, `,` and `/`.
	hosts := body[strings.LastIndexByte(body, '@')+1:]

	tail := ""
	if cut := strings.IndexAny(hosts, "/?#"); cut >= 0 {
		hosts, tail = hosts[:cut], hosts[cut:]
	}

	tls := pgScheme(scheme) && sslRequired(queryValue(tail, "sslmode"))

	if hosts == "" {
		if pgScheme(scheme) || scheme == "unix" {
			return []ref{{Key: key, Scheme: scheme, List: list, JDBC: jdbc, Socket: true, TLS: tls}}
		}

		return nil
	}

	var refs []ref

	for element := range strings.SplitSeq(hosts, ",") {
		host, port, ok := hostAndPort(element)
		if !ok {
			continue
		}

		refs = append(refs, ref{
			Key: key, Scheme: scheme, Host: host, Port: port, List: list, JDBC: jdbc,
			Socket: strings.HasPrefix(strings.ToLower(host), "%2f"), TLS: tls,
		})
	}

	return refs
}

// hostAndPort splits `host[:port]` or `[v6]:port`. A port that is not a number is dropped.
func hostAndPort(element string) (string, uint16, bool) {
	if element == "" {
		return "", 0, false
	}

	if strings.HasPrefix(element, "[") {
		host, after, found := strings.Cut(element[1:], "]")
		if !found {
			return "", 0, false
		}

		port, _ := parsePort(strings.TrimPrefix(after, ":"))

		return host, port, true
	}

	if strings.Count(element, ":") > 1 {
		return element, 0, true
	}

	host, portText, _ := strings.Cut(element, ":")
	port, _ := parsePort(portText)

	return host, port, host != ""
}

// keywordString reads a libpq keyword string: nothing but `key=value` pairs, one of them `host` or
// `hostaddr`, and one a connection keyword.
func keywordString(key, value string) ([]ref, bool) {
	pairs, ok := keywordPairs(value)
	if !ok {
		return nil, false
	}

	_, host := pairs["host"]
	_, hostaddr := pairs["hostaddr"]

	connection := false

	for _, name := range []string{"dbname", "user", "password", "port", "sslmode"} {
		_, found := pairs[name]
		connection = connection || found
	}

	if (!host && !hostaddr) || !connection {
		return nil, false
	}

	ports := strings.Split(pairs["port"], ",")
	tls := sslRequired(pairs["sslmode"])
	refs := pgHosts(key, pairs["host"], ports, tls)

	return append(refs, pgHosts(key, pairs["hostaddr"], ports, tls)...), true
}

// keywordPairs splits whitespace-separated `key=value` pairs, a value optionally single-quoted.
func keywordPairs(value string) (map[string]string, bool) {
	pairs := map[string]string{}
	rest := strings.TrimSpace(value)

	for rest != "" {
		name, after, found := strings.Cut(rest, "=")
		if !found || !keywordKey.MatchString(name) {
			return nil, false
		}

		text, consumed, ok := keywordValue(after)
		if !ok {
			return nil, false
		}

		pairs[name] = text
		rest = strings.TrimLeft(after[consumed:], " \t\r\n")
	}

	return pairs, len(pairs) > 0
}

// keywordValue reads one value — up to whitespace, or a single-quoted run with `\'` escapes — and
// the number of bytes it spans.
func keywordValue(text string) (string, int, bool) {
	if !strings.HasPrefix(text, "'") {
		if end := strings.IndexAny(text, " \t\r\n"); end >= 0 {
			return text[:end], end, true
		}

		return text, len(text), true
	}

	var out strings.Builder

	for index := 1; index < len(text); index++ {
		switch {
		case text[index] == '\\' && index+1 < len(text):
			index++
			out.WriteByte(text[index])
		case text[index] == '\'':
			return out.String(), index + 1, true
		default:
			out.WriteByte(text[index])
		}
	}

	return "", 0, false
}

// pgHosts reads a libpq host list; ports pairs with it by position, one port serving every host.
func pgHosts(key, hosts string, ports []string, tls bool) []ref {
	var refs []ref

	for index, host := range strings.Split(hosts, ",") {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}

		var port uint16

		switch {
		case len(ports) == 1:
			port, _ = parsePort(ports[0])
		case index < len(ports):
			port, _ = parsePort(ports[index])
		default:
		}

		refs = append(refs, ref{
			Key: key, Host: host, Port: port, KeywordPG: true,
			Socket: strings.HasPrefix(host, "/") || strings.HasPrefix(host, "@"), TLS: tls,
		})
	}

	return refs
}

// bareReferences reads the tokens left once URLs are removed: `name:port` and IP literals.
func bareReferences(key, rest string) []ref {
	var refs []ref

	for _, token := range strings.FieldsFunc(rest, func(r rune) bool {
		return strings.ContainsRune(" \t\r\n,;=", r)
	}) {
		if addrPort, err := netip.ParseAddrPort(token); err == nil {
			refs = append(refs, ref{Key: key, Host: addrPort.Addr().String(), Port: addrPort.Port()})

			continue
		}

		if addr, err := netip.ParseAddr(token); err == nil {
			refs = append(refs, ref{Key: key, Host: addr.String()})

			continue
		}

		match := namePort.FindStringSubmatch(token)
		if match == nil || strings.IndexFunc(match[1], isLetter) < 0 {
			continue
		}

		if port, ok := parsePort(match[2]); ok {
			refs = append(refs, ref{Key: key, Host: match[1], Port: port})
		}
	}

	return refs
}

func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// parsePort reads a decimal port 1-65535.
func parsePort(text string) (uint16, bool) {
	port, err := strconv.ParseUint(text, 10, 16)
	if err != nil || port == 0 {
		return 0, false
	}

	return uint16(port), true
}

// queryValue returns one parameter of a URL's `?query`, empty when absent.
func queryValue(tail, name string) string {
	_, query, found := strings.Cut(tail, "?")
	if !found {
		return ""
	}

	query, _, _ = strings.Cut(query, "#")

	for pair := range strings.SplitSeq(query, "&") {
		if key, value, _ := strings.Cut(pair, "="); key == name {
			return value
		}
	}

	return ""
}

func pgScheme(scheme string) bool {
	return scheme == schemePostgres || scheme == schemePostgresql
}

// sslRequired reports an sslmode under which the client refuses a cleartext connection.
func sslRequired(mode string) bool {
	return slices.Contains([]string{"require", "verify-ca", "verify-full"}, mode)
}
