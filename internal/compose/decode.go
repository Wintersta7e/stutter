package compose

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"strconv"
)

// errShape means a value is not in a shape any supported compose release prints. It names nothing
// of the value.
var errShape = errors.New("a value is not in the shape compose prints")

// flexInt is an integer compose prints as a number or, depending on the release, as a decimal
// string: `mem_limit` is 536870912 on one release and "536870912" on another.
type flexInt int64

// UnmarshalJSON accepts a number or a decimal string.
func (n *flexInt) UnmarshalJSON(data []byte) error {
	text := unquote(data)

	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return errShape
	}

	*n = flexInt(value)

	return nil
}

// flexFloat is a number compose prints as a number or as a decimal string.
type flexFloat float64

// UnmarshalJSON accepts a number or a decimal string.
func (n *flexFloat) UnmarshalJSON(data []byte) error {
	value, err := strconv.ParseFloat(unquote(data), 64)
	if err != nil {
		return errShape
	}

	*n = flexFloat(value)

	return nil
}

// fileMode is a permission mode. One compose release prints `0400` as the number 256, a later one
// as the octal string "0400".
type fileMode fs.FileMode

// UnmarshalJSON reads a number as decimal and a string as octal.
func (m *fileMode) UnmarshalJSON(data []byte) error {
	base := 10
	if len(data) > 0 && data[0] == '"' {
		base = 8
	}

	value, err := strconv.ParseUint(unquote(data), base, 32)
	if err != nil {
		return errShape
	}

	*m = fileMode(value)

	return nil
}

// ulimit is one `ulimits` entry: a single number sets soft and hard alike.
type ulimit [2]int64

// UnmarshalJSON accepts a number or `{soft, hard}`.
func (u *ulimit) UnmarshalJSON(data []byte) error {
	var single flexInt
	if err := single.UnmarshalJSON(data); err == nil {
		*u = ulimit{int64(single), int64(single)}

		return nil
	}

	var pair struct {
		Soft flexInt `json:"soft"`
		Hard flexInt `json:"hard"`
	}

	if err := json.Unmarshal(data, &pair); err != nil {
		return errShape
	}

	*u = ulimit{int64(pair.Soft), int64(pair.Hard)}

	return nil
}

// scalarText is a string-valued map entry compose may print as a number or a boolean, as
// `sysctls` values are.
type scalarText string

// UnmarshalJSON keeps a string's text and a number's or boolean's literal.
func (s *scalarText) UnmarshalJSON(data []byte) error {
	*s = scalarText(unquote(data))

	return nil
}

func unquote(data []byte) string {
	text := string(bytes.TrimSpace(data))
	if unquoted, err := strconv.Unquote(text); err == nil {
		return unquoted
	}

	return text
}

// composeProject is the typed form of a parsed model: the facts every later step reads. Keys it
// does not name are still in the generic form, and the closed walk has already vetted every key.
type composeProject struct {
	Services map[string]*composeService `json:"services"`
	Volumes  map[string]*topVolume      `json:"volumes"`
	Configs  map[string]*topFile        `json:"configs"`
	Secrets  map[string]*topFile        `json:"secrets"`
	Name     string                     `json:"name"`
}

// topVolume is a top-level named volume definition.
type topVolume struct {
	External any    `json:"external"`
	Name     string `json:"name"`
}

// topFile is a top-level config or secret definition.
type topFile struct {
	External       any     `json:"external"`
	Content        *string `json:"content"`
	File           string  `json:"file"`
	Environment    string  `json:"environment"`
	Driver         string  `json:"driver"`
	TemplateDriver string  `json:"template_driver"`
}

// composeService is the typed form of one service. A nil list pointer is a key compose printed as
// null or not at all: both mean unset.
type composeService struct {
	Environment       map[string]*string     `json:"environment"`
	Sysctls           map[string]scalarText  `json:"sysctls"`
	Labels            map[string]string      `json:"labels"`
	StorageOpt        map[string]string      `json:"storage_opt"`
	Ulimits           map[string]ulimit      `json:"ulimits"`
	Networks          map[string]*serviceNet `json:"networks"`
	DependsOn         map[string]dependency  `json:"depends_on"`
	Build             *serviceBuild          `json:"build"`
	Command           *[]string              `json:"command"`
	Entrypoint        *[]string              `json:"entrypoint"`
	Healthcheck       *serviceHealthcheck    `json:"healthcheck"`
	Deploy            *serviceDeploy         `json:"deploy"`
	BlkioConfig       *serviceBlkio          `json:"blkio_config"`
	MemSwappiness     *flexInt               `json:"mem_swappiness"`
	Scale             *flexInt               `json:"scale"`
	Image             string                 `json:"image"`
	Platform          string                 `json:"platform"`
	WorkingDir        string                 `json:"working_dir"`
	User              string                 `json:"user"`
	Domainname        string                 `json:"domainname"`
	MacAddress        string                 `json:"mac_address"`
	Hostname          string                 `json:"hostname"`
	ContainerName     string                 `json:"container_name"`
	IPC               string                 `json:"ipc"`
	Cgroup            string                 `json:"cgroup"`
	Pid               string                 `json:"pid"`
	UTS               string                 `json:"uts"`
	UsernsMode        string                 `json:"userns_mode"`
	Runtime           string                 `json:"runtime"`
	Isolation         string                 `json:"isolation"`
	NetworkMode       string                 `json:"network_mode"`
	PullPolicy        string                 `json:"pull_policy"`
	StopSignal        string                 `json:"stop_signal"`
	StopGracePeriod   string                 `json:"stop_grace_period"`
	Restart           string                 `json:"restart"`
	Cpuset            string                 `json:"cpuset"`
	CgroupParent      string                 `json:"cgroup_parent"`
	GroupAdd          []string               `json:"group_add"`
	Tmpfs             []string               `json:"tmpfs"`
	SecurityOpt       []string               `json:"security_opt"`
	CapAdd            []string               `json:"cap_add"`
	CapDrop           []string               `json:"cap_drop"`
	Links             []string               `json:"links"`
	ExternalLinks     []string               `json:"external_links"`
	Expose            []scalarText           `json:"expose"`
	Profiles          []string               `json:"profiles"`
	DNS               []string               `json:"dns"`
	VolumesFrom       []string               `json:"volumes_from"`
	DeviceCgroupRules []string               `json:"device_cgroup_rules"`
	Ports             []servicePort          `json:"ports"`
	Volumes           []serviceVolume        `json:"volumes"`
	Configs           []serviceFile          `json:"configs"`
	Secrets           []serviceFile          `json:"secrets"`
	CPUs              flexFloat              `json:"cpus"`
	CPUCount          flexInt                `json:"cpu_count"`
	CPUPercent        flexInt                `json:"cpu_percent"`
	CPUPeriod         flexInt                `json:"cpu_period"`
	CPUQuota          flexInt                `json:"cpu_quota"`
	CPURTPeriod       flexInt                `json:"cpu_rt_period"`
	CPURTRuntime      flexInt                `json:"cpu_rt_runtime"`
	CPUShares         flexInt                `json:"cpu_shares"`
	MemLimit          flexInt                `json:"mem_limit"`
	MemReservation    flexInt                `json:"mem_reservation"`
	MemSwapLimit      flexInt                `json:"memswap_limit"`
	ShmSize           flexInt                `json:"shm_size"`
	OomScoreAdj       flexInt                `json:"oom_score_adj"`
	PidsLimit         flexInt                `json:"pids_limit"`
	Init              bool                   `json:"init"`
	ReadOnly          bool                   `json:"read_only"`
	StdinOpen         bool                   `json:"stdin_open"`
	TTY               bool                   `json:"tty"`
	Privileged        bool                   `json:"privileged"`
	OomKillDisable    bool                   `json:"oom_kill_disable"`
	UseAPISocket      bool                   `json:"use_api_socket"`
}

// serviceBuild is the part of `build` that identifies what is built. Everything else of it is
// handed to compose verbatim.
type serviceBuild struct {
	Context          string `json:"context"`
	Dockerfile       string `json:"dockerfile"`
	DockerfileInline string `json:"dockerfile_inline"`
	Target           string `json:"target"`
}

// serviceNet is one entry of a service's `networks`; null is a network joined with no options.
type serviceNet struct {
	Aliases []string `json:"aliases"`
}

// dependency is one `depends_on` entry.
type dependency struct {
	Condition string `json:"condition"`
}

// serviceHealthcheck is `healthcheck` as compose normalises it: the test always a list, timings Go
// duration strings.
type serviceHealthcheck struct {
	Interval      string   `json:"interval"`
	Timeout       string   `json:"timeout"`
	StartPeriod   string   `json:"start_period"`
	StartInterval string   `json:"start_interval"`
	Test          []string `json:"test"`
	Retries       flexInt  `json:"retries"`
	Disable       bool     `json:"disable"`
}

// serviceDeploy is the part of `deploy` a single container honours or replaces.
type serviceDeploy struct {
	Replicas  *flexInt        `json:"replicas"`
	Mode      string          `json:"mode"`
	Resources deployResources `json:"resources"`
}

// deployResources is `deploy.resources`.
type deployResources struct {
	Limits       resourceSet `json:"limits"`
	Reservations resourceSet `json:"reservations"`
}

// resourceSet is `limits` or `reservations`.
type resourceSet struct {
	CPUs   flexFloat `json:"cpus"`
	Memory flexInt   `json:"memory"`
	Pids   flexInt   `json:"pids"`
}

// serviceBlkio is `blkio_config`.
type serviceBlkio struct {
	WeightDevice    []blkioWeight `json:"weight_device"`
	DeviceReadBps   []blkioRate   `json:"device_read_bps"`
	DeviceReadIOps  []blkioRate   `json:"device_read_iops"`
	DeviceWriteBps  []blkioRate   `json:"device_write_bps"`
	DeviceWriteIOps []blkioRate   `json:"device_write_iops"`
	Weight          flexInt       `json:"weight"`
}

type blkioWeight struct {
	Path   string  `json:"path"`
	Weight flexInt `json:"weight"`
}

type blkioRate struct {
	Path string  `json:"path"`
	Rate flexInt `json:"rate"`
}

// servicePort is one long-form `ports` entry.
type servicePort struct {
	Published string  `json:"published"`
	Protocol  string  `json:"protocol"`
	Target    flexInt `json:"target"`
}

// serviceVolume is one long-form `volumes` entry, as compose normalises every short form.
type serviceVolume struct {
	Bind     *volumeBind   `json:"bind"`
	Volume   *volumeVolume `json:"volume"`
	Tmpfs    *volumeTmpfs  `json:"tmpfs"`
	Image    *volumeImage  `json:"image"`
	Type     string        `json:"type"`
	Source   string        `json:"source"`
	Target   string        `json:"target"`
	ReadOnly bool          `json:"read_only"`
}

// volumeBind keeps the bind options Stutter acts on; `create_host_path` and `propagation` change
// nothing, and one release prints `create_host_path` where another prints nothing.
type volumeBind struct {
	Recursive string `json:"recursive"`
	SELinux   string `json:"selinux"`
}

type volumeVolume struct {
	Subpath string `json:"subpath"`
	NoCopy  bool   `json:"nocopy"`
}

type volumeTmpfs struct {
	Size flexInt  `json:"size"`
	Mode fileMode `json:"mode"`
}

type volumeImage struct {
	Subpath string `json:"subpath"`
}

// serviceFile is one service `configs` or `secrets` entry.
type serviceFile struct {
	Mode   *fileMode `json:"mode"`
	Source string    `json:"source"`
	Target string    `json:"target"`
	UID    string    `json:"uid"`
	GID    string    `json:"gid"`
}
