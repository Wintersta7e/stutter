package compose

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestASpecAndACopyInNeverPrintTheirValues(t *testing.T) {
	t.Parallel()

	const secretKey = "PASS"

	sentinel := rand.Text()
	copyIn := CopyIn{Target: "/run/secrets/tok", UID: 1000, GID: 1000, Mode: 0o400, data: []byte(sentinel)}
	spec := Spec{
		Service:    testTarget,
		Image:      "sha256:0123",
		Env:        map[string]string{secretKey: sentinel, "PLAIN": "x"},
		raw:        map[string]string{secretKey: sentinel},
		Sysctls:    map[string]string{"net.core.somaxconn": "1024"},
		Entrypoint: []string{"sh", "-c", sentinel},
		Cmd:        []string{sentinel},
		Mounts:     []Mount{{Kind: MountBind, Source: "/project/conf", Target: "/conf", ReadOnly: true}},
		CopyIn:     []CopyIn{copyIn},
		Unset:      []string{"HTTP_PROXY"},
		target:     true,
	}

	// Reflection prints a []byte as a list of numbers, so each of its spellings is searched for.
	decimal := make([]string, len(sentinel))
	hex := make([]string, len(sentinel))

	for index := range len(sentinel) {
		decimal[index] = strconv.Itoa(int(sentinel[index]))
		hex[index] = fmt.Sprintf("%#x", sentinel[index])
	}

	spellings := []string{sentinel, strings.Join(decimal, " "), strings.Join(hex, ", ")}
	values := map[string]any{"Spec": spec, "CopyIn": copyIn}
	formats := 0

	for name, value := range values {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			formats++

			printed := fmt.Sprintf(verb, value)
			for _, spelling := range spellings {
				if at := strings.Index(printed, spelling); at >= 0 {
					t.Errorf("%s printed with %s carries the value at byte %d: %s", name, verb, at, printed)
				}
			}
		}
	}

	pointer := fmt.Sprintf("%+v", &spec)
	if strings.Contains(pointer, sentinel) {
		t.Errorf("*Spec printed with %%+v carries the value: %s", pointer)
	}

	for _, key := range []string{secretKey, "PLAIN", "/run/secrets/tok", "/conf"} {
		if !strings.Contains(fmt.Sprintf("%v", spec), key) {
			t.Errorf("Spec printed with %%v does not name %q: %v", key, spec)
		}
	}

	t.Logf("formats=%d", formats)

	if formats < 8 {
		t.Fatalf("formats=%d, want 8", formats)
	}
}
