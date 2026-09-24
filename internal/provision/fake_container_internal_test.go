//go:build linux

package provision

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// valueless are the create flags that take no value.
func valueless() map[string]bool {
	return map[string]bool{
		"--init": true, "--read-only": true, "-i": true, "-t": true, "--no-healthcheck": true,
		"--oom-kill-disable": true,
	}
}

// errNoImage means a create argv named no image.
var errNoImage = errors.New("no image")

// fakeCreate is a container as the fake engine reads it from its create argv.
type fakeCreate struct {
	inspect *containerReport
	flags   map[string][]string
	name    string
	anon    []string
}

// create lands a container whose inspect answers what its argv asked for, as the engine would.
func (f *fakeEngine) create(req request, rest []string) (result, error) {
	c, err := f.parseCreate(rest)
	if err != nil {
		return result{exit: 1, stderrFirst: err.Error()}, &CallError{Verb: "create", Code: 1, Stderr: err.Error()}
	}

	c.inspect.Env = createEnv(req, c.flags["-e"])
	c.inspect.ID = f.mintID()
	f.objects[ResourceContainer][c.inspect.ID] = &fakeObject{
		id: c.inspect.ID, name: c.name, labels: c.inspect.Labels, inspect: c.inspect, anon: c.anon,
	}

	return result{out: []byte(c.inspect.ID + "\n")}, nil
}

// createEnv is the environment the container gets: the env-file lines, then each `-e KEY` whose
// value the child environment carries. A bare `-e NAME` with no value unsets it.
func createEnv(req request, names []string) []string {
	var env []string

	if req.stdin != nil {
		data, err := io.ReadAll(req.stdin)
		if err == nil {
			env = append(env, strings.FieldsFunc(string(data), func(r rune) bool { return r == '\n' })...)
		}
	}

	for _, name := range names {
		if value := lookupEnv(req.extraEnv, name); value != "" {
			env = append(env, name+"="+value)
		}
	}

	return env
}

func (f *fakeEngine) parseCreate(rest []string) (*fakeCreate, error) {
	c := &fakeCreate{flags: map[string][]string{}}
	i := 0

	for ; i < len(rest) && strings.HasPrefix(rest[i], "-"); i++ {
		flag := rest[i]
		if valueless()[flag] {
			c.flags[flag] = append(c.flags[flag], "")

			continue
		}

		if i+1 >= len(rest) {
			return nil, fmt.Errorf("flag %s has no value", flag)
		}

		i++
		c.flags[flag] = append(c.flags[flag], rest[i])
	}

	if i >= len(rest) {
		return nil, errNoImage
	}

	c.name = first(c.flags["--name"])
	labels := map[string]string{}

	for _, label := range c.flags["--label"] {
		key, value, _ := strings.Cut(label, "=")
		labels[key] = value
	}

	c.inspect = &containerReport{
		Image: rest[i], Cmd: rest[i+1:], Labels: labels, Restart: first(c.flags["--restart"]),
		LogDriver: first(c.flags["--log-driver"]), CapAdd: c.flags["--cap-add"], User: first(c.flags["-u"]),
		WorkingDir: first(c.flags["-w"]), Hostname: first(c.flags["--hostname"]),
		Entrypoint: c.flags["--entrypoint"], Healthcheck: healthOf(c.flags), IpcMode: "private",
		CgroupnsMode: "private",
	}

	c.inspect.Networks = createNetworks(c.flags["--network"])
	c.inspect.Bindings, c.inspect.Ports = createBindings(c.flags["-p"]), createPorts(c.flags["-p"])

	return c, f.createMounts(c)
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}

	return values[0]
}

func healthOf(flags map[string][]string) []string {
	if _, off := flags["--no-healthcheck"]; off {
		return []string{"NONE"}
	}

	if cmd := first(flags["--health-cmd"]); cmd != "" {
		return []string{"CMD-SHELL", cmd}
	}

	return nil
}

// csvFields splits one --network or --mount value as the engine does.
func csvFields(value string) map[string][]string {
	fields, err := csv.NewReader(strings.NewReader(value)).Read()
	out := map[string][]string{}

	if err != nil {
		return out
	}

	for _, field := range fields {
		key, val, _ := strings.Cut(field, "=")
		out[key] = append(out[key], val)
	}

	return out
}

func createNetworks(values []string) []endpointReport {
	networks := make([]endpointReport, 0, len(values))

	for n, value := range values {
		if value == "none" {
			networks = append(networks, endpointReport{Name: value})

			continue
		}

		fields := csvFields(value)
		networks = append(networks, endpointReport{
			Name: first(fields["name"]), IP: "10.231.20." + strconv.Itoa(n+2), Aliases: fields["alias"],
		})
	}

	return networks
}

// createBindings reads each `-p ip:hostport:port` as the create's port binding.
func createBindings(values []string) []portReport {
	bindings := make([]portReport, 0, len(values))

	for _, value := range values {
		parts := strings.Split(value, ":")
		bindings = append(bindings, portReport{Port: parts[len(parts)-1], HostIP: parts[0], HostPort: parts[1]})
	}

	return bindings
}

// createPorts publishes each binding on an engine-assigned host port.
func createPorts(values []string) []portReport {
	ports := createBindings(values)
	for n := range ports {
		ports[n].HostPort = strconv.Itoa(49153 + n)
	}

	return ports
}

// createMounts reads every --mount; an unnamed volume mount becomes an anonymous volume carrying its
// volume-label values, as the engine makes one.
func (f *fakeEngine) createMounts(c *fakeCreate) error {
	for _, value := range c.flags["--mount"] {
		fields := csvFields(value)
		dst := first(fields["dst"])
		_, readOnly := fields["readonly"]

		switch first(fields["type"]) {
		case mountBind:
			c.inspect.Mounts = append(c.inspect.Mounts, mountReport{
				Type: mountBind, Source: first(fields["src"]), Destination: dst, RW: !readOnly,
			})
			_, recursive := fields["bind-recursive"]
			c.inspect.HostMounts = append(c.inspect.HostMounts, hostMountReport{Target: dst, Recursive: recursive})
		case mountVolume:
			name := first(fields["src"])
			if name == "" {
				name = f.anonymousVolume(fields["volume-label"])
				c.anon = append(c.anon, name)
			}

			c.inspect.Mounts = append(c.inspect.Mounts, mountReport{
				Type: mountVolume, Name: name, Destination: dst, RW: !readOnly,
			})
		case mountTmpfs:
			c.inspect.Mounts = append(c.inspect.Mounts, mountReport{Type: mountTmpfs, Destination: dst, RW: true})
		default:
			return fmt.Errorf("mount type %q", first(fields["type"]))
		}
	}

	return nil
}

// anonymousVolume lands an anonymous volume carrying the given volume-label values.
func (f *fakeEngine) anonymousVolume(volumeLabels []string) string {
	name := strings.Repeat("0", 32) + f.mintID()[32:]
	labels := map[string]string{"com.docker.volume.anonymous": ""}

	for _, label := range volumeLabels {
		key, val, _ := strings.Cut(label, "=")
		labels[key] = val
	}

	f.objects[ResourceVolume][name] = &fakeObject{id: name, name: name, labels: labels}

	return name
}

// cloneReport copies a container's inspect deep enough for a test to edit it.
func cloneReport(r containerReport) containerReport {
	r.Mounts, r.HostMounts = slices.Clone(r.Mounts), slices.Clone(r.HostMounts)
	r.Networks, r.Bindings, r.Ports = slices.Clone(r.Networks), slices.Clone(r.Bindings), slices.Clone(r.Ports)

	return r
}

// copyIn keeps the tar a container was sent.
func (f *fakeEngine) copyIn(spec verbSpec, stdin io.Reader, rest []string) (result, error) {
	id, _, _ := strings.Cut(rest[0], ":")

	container := f.find(ResourceContainer, id)
	if container == nil {
		return fail(spec, "No such container: "+id)
	}

	data, err := io.ReadAll(stdin)
	if err != nil {
		return result{}, err
	}

	container.copied = data

	return result{}, nil
}

// copyOut answers with the tar the container was last sent.
func (f *fakeEngine) copyOut(spec verbSpec, rest []string) (result, error) {
	id, _, _ := strings.Cut(rest[0], ":")

	container := f.find(ResourceContainer, id)
	if container == nil {
		return fail(spec, "No such container: "+id)
	}

	return result{out: container.copied}, nil
}
