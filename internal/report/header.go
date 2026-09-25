package report

import (
	"slices"
	"strconv"
	"strings"

	"github.com/Wintersta7e/stutter/internal/compose"
	"github.com/Wintersta7e/stutter/internal/corpus"
	"github.com/Wintersta7e/stutter/internal/harness"
	"github.com/Wintersta7e/stutter/internal/provision"
	httpproxy "github.com/Wintersta7e/stutter/internal/proxy/http"
	"github.com/Wintersta7e/stutter/internal/relay"
)

// Header is what a compose report states before any consumer: what the check was pointed at, what it
// changed, and what it cannot observe. Each field is its producer's own value. A field still nil or
// zero belongs to a step the check never reached, and its row is left out — except the disclosure
// block, which renders whenever there is a header.
//
// Nothing here renders a value from an environment, a DSN, a build argument, a secret or the compose
// model: names, keys, paths and counts only.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Header struct {
	// Model is the parsed compose project.
	Model *compose.Model
	// Timings are the waits the consumer checks used, as the harness applied them.
	Timings *harness.Timings
	// Engine is the engine the check ran against.
	Engine *provision.Identity
	// Image is the image pinned for the service under test.
	Image *compose.Image
	// Classification is every compose service classified.
	Classification *compose.Classification
	// Seeds are how each started dependency was seeded; empty when none was started.
	Seeds *[]provision.SeedRecord
	// Spec is the container the service under test runs as.
	Spec *compose.Spec
	// Prints are the bind sources fingerprinted before anything was created.
	Prints *compose.Prints
	// Corpus is the corpus as loaded.
	Corpus *corpus.Loaded
	// Hosts are what the stub answered per external host, summed over every consumer check.
	Hosts *[]httpproxy.HostTally
	// Counts are the connections the listeners refused.
	Counts *harness.ListenerCounts
	// Queries are the DNS questions the stub relay answered with no address.
	Queries []relay.Query
	// Target is how the service under test's image is resolved.
	Target compose.ImageRef
	// Given is what the command line named.
	Given Given
	// Mode is how containers reach the listeners; zero before the listeners opened.
	Mode harness.Mode
}

// Given is what the command line named, as given.
//
// Field order is dictated by govet's fieldalignment check, not by reading order.
type Given struct {
	// Stream is --stream.
	Stream string
	// Corpus is --corpus.
	Corpus string
	// Routes is --routes; empty when not given.
	Routes string
	// Compose are the --compose files in the order given.
	Compose []string
	// Profiles are the --profile values given.
	Profiles []string
	// Consumers are the --consumer names given.
	Consumers []string
	// Timings names each timing flag given, as --startup, --quiesce or --drain.
	Timings []string
}

// lines renders every row the check reached, in the header's order.
func (h *Header) lines() []string {
	if h == nil {
		return nil
	}

	rows := []func() []string{
		h.inputs, h.declarations, h.engine, h.image, h.hostMode, h.dependencies, h.seeds,
		h.specChanges, h.corpus, h.hosts, h.disclosure, h.reach, h.setupEgress,
	}

	var lines []string

	for _, row := range rows {
		lines = append(lines, row()...)
	}

	return lines
}

// inputs names the compose files in the order given, the project, the active profiles and the services
// each brought in, the COMPOSE_* variables in effect, whether a .env was read, and how many lines
// compose printed on stderr — whose text is never kept.
func (h *Header) inputs() []string {
	if h.Model == nil {
		return nil
	}

	model := h.Model
	lines := []string{rowInputs + " project " + model.Project() + ", " + plural(len(h.Given.Compose), "file")}

	for _, file := range h.Given.Compose {
		lines = append(lines, detailIndent+"file "+file)
	}

	for _, profile := range model.Profiles() {
		lines = append(lines, detailIndent+"profile "+profile.Name+": "+strings.Join(profile.Services, ", "))
	}

	if names := model.ComposeVars(); len(names) > 0 {
		lines = append(lines, detailIndent+"COMPOSE_* variables in effect: "+strings.Join(names, ", "))
	}

	dotEnv := "no .env read"
	if model.DotEnv() {
		dotEnv = ".env read"
	}

	return append(lines, detailIndent+dotEnv+"; compose printed "+plural(model.StderrLines(), "line")+" on stderr")
}

// declarations names each x-stutter entry with its file, and each optional flag given, a timing with
// the value the harness used rather than the text typed.
func (h *Header) declarations() []string {
	if h.Model == nil {
		return nil
	}

	var lines []string

	if h.Classification != nil {
		for _, declared := range h.Classification.Declarations {
			lines = append(lines, detailIndent+"x-stutter "+declared.Service+" "+declared.Key+" → "+declared.Value+
				" ("+h.givenFile(declared.File)+")")
		}
	}

	if h.Given.Routes != "" {
		lines = append(lines, detailIndent+"--routes "+h.Given.Routes)
	}

	if len(h.Given.Consumers) > 0 {
		lines = append(lines, detailIndent+"--consumer "+strings.Join(h.Given.Consumers, ", "))
	}

	if len(h.Given.Profiles) > 0 {
		lines = append(lines, detailIndent+"--profile "+strings.Join(h.Given.Profiles, ", "))
	}

	lines = append(lines, h.timingLines()...)

	head := rowDeclarations + " " + plural(len(lines), "declaration")
	if len(lines) == 0 {
		head = rowDeclarations + " none beyond the four names"
	}

	return append([]string{head}, lines...)
}

// timingLines render each timing flag given with the value the harness applied.
func (h *Header) timingLines() []string {
	if h.Timings == nil {
		return nil
	}

	applied := map[string]string{
		"--startup": h.Timings.Startup.String(),
		"--quiesce": h.Timings.Quiesce.String(),
		"--drain":   h.Timings.Drain.String(),
	}

	var lines []string

	for _, flag := range h.Given.Timings {
		if value, known := applied[flag]; known {
			lines = append(lines, detailIndent+flag+" "+value)
		}
	}

	return lines
}

// givenFile names a compose file as the command line gave it, where the model holds it absolute.
func (h *Header) givenFile(file string) string {
	if at := slices.Index(h.Model.Files(), file); at >= 0 && at < len(h.Given.Compose) {
		return h.Given.Compose[at]
	}

	return file
}

// engine identifies the engine: CLI, server and API versions, platform, endpoint, context and ID.
func (h *Header) engine() []string {
	if h.Engine == nil {
		return nil
	}

	id := h.Engine

	return []string{
		rowEngine + " Docker " + id.ServerVersion + " (API " + id.APIVersion + ") on " + id.OS + "/" + id.Arch,
		detailIndent + "platform " + id.Platform + "; compose " + id.Compose,
		detailIndent + "CLI " + id.CLIPath + " " + id.CLIVersion,
		detailIndent + "endpoint " + id.Endpoint + ", context " + id.Context,
		detailIndent + "engine ID " + id.EngineID,
	}
}

// image names the service under test, its image reference, the pinned image ID and its platform.
func (h *Header) image() []string {
	if h.Image == nil {
		return nil
	}

	reference := h.Target.Ref
	if h.Target.Build {
		reference = "built from its build key"
	}

	platform := h.Image.OS + "/" + h.Image.Arch
	if h.Image.Variant != "" {
		platform += "/" + h.Image.Variant
	}

	return []string{
		rowImage + " service " + h.Target.Service + ", image " + reference,
		detailIndent + "pinned " + h.Image.ID + ", platform " + platform,
	}
}

// hostMode names how containers reach the listeners, and the limit that mode carries.
func (h *Header) hostMode() []string {
	if h.Mode == 0 {
		return nil
	}

	lines := []string{rowHostMode + " " + h.Mode.String()}
	if h.Mode == harness.ModeHostAlias {
		lines = append(lines, detailIndent+halfCloseLimit)
	}

	return lines
}

// dependencies is the classification table: every service besides the one under test, its role, its
// endpoints as port, protocol and TLS, and whether it is started.
func (h *Header) dependencies() []string {
	if h.Classification == nil {
		return nil
	}

	started := h.Classification.Started()
	lines := []string{rowDependencies + " " + plural(len(h.Classification.Deps), "service") +
		" besides the one under test; " + strconv.Itoa(max(len(started)-1, 0)) + " started with it"}

	for _, dep := range h.Classification.Deps {
		line := detailIndent + dep.Service + " (" + string(dep.Role) + ")"
		if endpoints := endpointList(dep.Endpoints); endpoints != "" {
			line += ": " + endpoints
		}

		if !slices.Contains(started, dep.Service) {
			line += "; not started"
		}

		lines = append(lines, line)
	}

	return lines
}

// endpointList renders endpoints as port/protocol, with /tls on an encrypted one.
func endpointList(endpoints []compose.Endpoint) string {
	parts := make([]string, 0, len(endpoints))

	for _, endpoint := range endpoints {
		part := strconv.Itoa(int(endpoint.Port)) + "/" + string(endpoint.Protocol)
		if endpoint.TLS {
			part += "/tls"
		}

		parts = append(parts, part)
	}

	return strings.Join(parts, ", ")
}

// seeds is one row per started dependency: how it was seeded, or that nothing was found to seed it.
func (h *Header) seeds() []string {
	if h.Seeds == nil {
		return nil
	}

	records := *h.Seeds
	if len(records) == 0 {
		return []string{rowSeeds + " none found"}
	}

	lines := []string{rowSeeds + " " + plural(len(records), "started dependency")}

	for _, record := range records {
		lines = append(lines, detailIndent+record.Service+" ("+record.Path+"): "+seedText(record))
	}

	return lines
}

// seedText is what one seed did: its jobs, init scripts, mounts, promotions, silent ports and stop.
func seedText(record provision.SeedRecord) string {
	var parts []string

	if len(record.Jobs) > 0 {
		parts = append(parts, "jobs "+strings.Join(record.Jobs, ", "))
	}

	if record.InitScripts {
		parts = append(parts, "init scripts")
	}

	if len(record.Mounts) > 0 {
		parts = append(parts, "mounts "+strings.Join(record.Mounts, ", "))
	}

	if len(record.Promoted) > 0 {
		parts = append(parts, "promoted to Postgres "+ports(record.Promoted))
	}

	if len(record.NotListening) > 0 {
		parts = append(parts, "not listening "+ports(record.NotListening))
	}

	if record.Killed {
		parts = append(parts, "killed at the end of its grace period")
	}

	if len(parts) == 0 {
		return "nothing found to seed it"
	}

	return strings.Join(parts, "; ")
}

func ports(list []uint16) string {
	parts := make([]string, 0, len(list))
	for _, port := range list {
		parts = append(parts, strconv.Itoa(int(port)))
	}

	return strings.Join(parts, ", ")
}

// specChanges names every change Stutter made to the service under test's container: each replaced
// key, the CA variables set, the proxy variables removed, the binds forced read-only, the volumes
// substituted and the binds shared with a started service.
func (h *Header) specChanges() []string {
	if h.Spec == nil {
		return nil
	}

	var lines []string

	for _, replaced := range h.Spec.Replaced {
		lines = append(lines, detailIndent+replaced.Key+" → "+replaced.What)
	}

	variables := compose.CAVariables()
	lines = append(lines, detailIndent+"CA variables set: "+strings.Join(variables[:], ", "))

	for _, named := range h.Spec.CAOverridden {
		lines = append(lines, detailIndent+"CA variable "+named.Name+" overridden ("+named.Source+" value)")
	}

	for _, named := range h.Spec.ProxyRemoved {
		lines = append(lines, detailIndent+"proxy variable "+named.Name+" removed ("+named.Source+" value)")
	}

	lines = append(lines, h.mountLines()...)

	return append([]string{rowSpec + " " + plural(len(lines), "change")}, lines...)
}

// mountLines name the binds forced read-only, with what the start fingerprint counted beneath them,
// the volumes substituted, and the binds the target shares with a started service.
func (h *Header) mountLines() []string {
	var lines []string

	for _, mount := range h.Spec.Mounts {
		switch mount.Kind { //nolint:exhaustive // a tmpfs is what compose declared: nothing was changed.
		case compose.MountBind:
			lines = append(lines, detailIndent+"bind "+mount.Target+" from "+mount.Source+" mounted read-only")
		case compose.MountFresh:
			lines = append(lines, detailIndent+"volume at "+mount.Target+" substituted by a fresh one per start")
		default:
		}
	}

	if h.Prints != nil && h.Prints.Sources() > 0 {
		lines = append(lines, detailIndent+plural(h.Prints.Sources(), "bind source")+" fingerprinted, "+
			plural(h.Prints.Entries(), "entry")+" beneath them")
	}

	if h.Classification != nil {
		for _, shared := range h.Classification.Shared {
			what := "bind " + shared.Path
			if shared.Volume {
				what = "volume " + shared.Path
			}

			lines = append(lines, detailIndent+what+" shared with "+shared.Service)
		}
	}

	return lines
}

// corpus names the stream and the corpus directory as given, and what was read from it.
func (h *Header) corpus() []string {
	if h.Corpus == nil {
		return nil
	}

	return []string{
		rowCorpus + " stream " + h.Given.Stream + ", directory " + h.Given.Corpus,
		detailIndent + plural(
			len(h.Corpus.Messages),
			"message",
		) + ", " + plural(
			h.Corpus.Sidecars,
			".headers sidecar",
		) +
			", " + plural(
			h.Corpus.Skipped,
			"dotfile",
		) + " skipped",
	}
}

// hosts names every external host the stub answered over the check, with how it answered.
func (h *Header) hosts() []string {
	if h.Hosts == nil {
		return nil
	}

	tallies := *h.Hosts
	if len(tallies) == 0 {
		return []string{rowHosts + " none answered"}
	}

	lines := []string{rowHosts + " " + plural(len(tallies), "host") + " answered by the stub"}

	for _, tally := range tallies {
		transport := "cleartext"
		if tally.TLS {
			transport = "TLS"
		}

		answered := "default reply"
		if tally.Routed > 0 {
			answered = "routed " + strconv.Itoa(tally.Routed) + " of them"
		}

		lines = append(lines, detailIndent+tally.Host+": "+plural(tally.Calls, "call")+" over "+transport+", "+
			answered+", "+strconv.Itoa(tally.OffScript)+" off-script")
	}

	return lines
}

// disclosure is the fixed block of what a compose check never observes, rendered even when there is
// nothing of each kind to name.
func (h *Header) disclosure() []string {
	lines := []string{
		rowDisclosure + " what this check cannot see",
		detailIndent + disclosureUDP,
		detailIndent + disclosureLiterals,
		detailIndent + disclosureLoopback,
		detailIndent + reservedPorts(relay.ReservedFirst, relay.ReservedLast),
	}

	for _, query := range h.Queries {
		lines = append(lines, detailIndent+"DNS "+query.Name+" type "+strconv.Itoa(int(query.Type))+" answered empty")
	}

	if h.Classification == nil {
		return lines
	}

	for _, host := range h.Classification.Disclosed {
		lines = append(lines, detailIndent+host.Service+" "+host.Key+" names "+host.Host+", which bypasses DNS")
	}

	for _, dep := range h.Classification.Deps {
		switch {
		case dep.Role == compose.RoleSibling:
			lines = append(lines, detailIndent+dep.Service+" is another consumer of the bus: stubbed, never started")
		case slices.ContainsFunc(dep.Endpoints, opaque):
			lines = append(lines, detailIndent+dep.Service+" is observed as bytes only: a divergence there is "+
				"reported but not described")
		default:
		}
	}

	return lines
}

func opaque(endpoint compose.Endpoint) bool {
	return endpoint.Protocol == compose.ProtocolOpaque
}

// reach says who could reach the proxies, and what the listeners refused.
func (h *Header) reach() []string {
	if h.Counts == nil {
		return nil
	}

	return []string{
		rowReach + " " + reachSentence,
		detailIndent + plural(h.Counts.Foreign, "foreign connection") + " refused; " +
			plural(h.Counts.Unattached, "connection") + " while no run was attached",
	}
}

// setupEgress names the hosts jobs and dependencies may reach during setup, which nothing observes.
func (h *Header) setupEgress() []string {
	if h.Classification == nil {
		return nil
	}

	lines := []string{rowSetupEgress + " " + setupEgressSentence}

	for _, host := range h.Classification.SetupEgress {
		lines = append(lines, detailIndent+host.Service+" "+host.Key+" names "+host.Host)
	}

	return lines
}
