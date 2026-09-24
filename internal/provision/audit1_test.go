package provision_test

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The static audit of what Stutter may run and remove: every tracked Go file, workflow, Makefile and
// script, from `git ls-files`. It needs no engine and runs in the normal gate.

const (
	runnerFile = "internal/provision/runner.go"
	ownerFile  = "internal/provision/owner.go"
	argvFile   = "internal/provision/argv.go"
	auditFile  = "internal/provision/audit1_test.go"
	decoyFile  = "internal/dockertest/docker.go"
)

// spawnersAllowed are the only files that may start a process, by path — never by a _test.go
// suffix — with why.
func spawnersAllowed() map[string]string {
	return map[string]string{
		runnerFile:                                     "the one production spawner",
		"cmd/stutter/main_internal_test.go":            "re-executes the binary to prove a second interrupt ends it",
		"internal/provision/enginetest/helper_test.go": "the lock-holding helper process",
		auditFile:                      "runs git ls-files",
		decoyFile:                      "the test-decoy helper",
		"internal/dockertest/build.go": "the build helper",
	}
}

// pruneExempt are the files that may name prune: this audit, the recorder that classifies prune
// API paths, and the audit-line grammar whose recorder line counts them.
func pruneExempt() map[string]bool {
	return map[string]bool{
		auditFile: true, "internal/dockertest/recorder.go": true,
		"internal/dockertest/recorder_internal_test.go": true, "internal/testgate/audit.go": true,
	}
}

// expectedVerbs is the admitted verb table, row by row, as the audit renders runner.go's verbs().
// A verb added to the runner but not here fails the audit.
func expectedVerbs() []string {
	const (
		user   = " mode=modeUser"
		made   = " mode=modeConstructed"
		short  = " deadline=deadlineShort"
		first  = " stderr=stderrFirstLine"
		docker = " program=programDocker"
		held   = " mutates=true hold=true"
		read   = " mutates=false hold=false"
	)

	return []string{
		"verbContext name=context" + docker + " prefix=[context inspect --format] suffix=[] alt=[]" + user + short +
			first + read,
		"verbVersion name=version" + docker + " prefix=[version --format] suffix=[] alt=[]" + user + short + first +
			read,
		"verbInfo name=info" + docker + " prefix=[info --format] suffix=[] alt=[]" + user + short + first + read,
		"verbWSLInfo name=wslinfo program=programWSLInfo prefix=[--networking-mode] suffix=[] alt=[]" + user + short +
			first + read,
		"verbComposeConfig name=composeConfig program=programCompose prefix=[compose] suffix=[config --format json]" +
			" alt=[config --environment]" + user + short + " stderr=stderrCount" + read,
		"verbInspect name=inspect" + docker + " prefix=[inspect --type container --format] suffix=[] alt=[]" + made +
			short + first + read,
		"verbImageInspect name=imageInspect" + docker + " prefix=[image inspect --format] suffix=[] alt=[]" + made +
			short + first + read,
		"verbNetworkInspect name=networkInspect" + docker + " prefix=[network inspect --format] suffix=[] alt=[]" +
			made + short + first + read,
		"verbVolumeInspect name=volumeInspect" + docker + " prefix=[volume inspect --format] suffix=[] alt=[]" + made +
			short + first + read,
		"verbNetworkList name=networkList" + docker + " prefix=[network ls --no-trunc --format] suffix=[] alt=[]" +
			made + short + first + read,
		"verbNetworkCreate name=networkCreate" + docker + " prefix=[network create] suffix=[] alt=[]" + made + short +
			first + held,
		"verbVolumeCreate name=volumeCreate" + docker + " prefix=[volume create] suffix=[] alt=[]" + made + short +
			first + held,
		"verbRemove name=remove" + docker + " prefix=[rm -f -v] suffix=[] alt=[]" + made + short + first + held,
		"verbNetworkRemove name=networkRemove" + docker + " prefix=[network rm] suffix=[] alt=[]" + made + short +
			first + held,
		"verbVolumeRemove name=volumeRemove" + docker + " prefix=[volume rm] suffix=[] alt=[]" + made + short + first +
			held,
		"verbImageRemove name=imageRemove" + docker + " prefix=[rmi --no-prune] suffix=[] alt=[]" + made + short +
			first + held,
		"verbContainerList name=containerList" + docker + " prefix=[ps -a --no-trunc --format] suffix=[] alt=[]" +
			made + short + first + read,
		"verbVolumeList name=volumeList" + docker + " prefix=[volume ls --format] suffix=[] alt=[]" + made + short +
			first + read,
		"verbImageList name=imageList" + docker + " prefix=[images --no-trunc --format] suffix=[] alt=[]" + made +
			short + first + read,
		"verbKill name=kill" + docker + " prefix=[stop --signal KILL] suffix=[] alt=[]" + made + short + first + held,
		"verbPull name=pull" + docker + " prefix=[pull -q] suffix=[] alt=[]" + user + " deadline=deadlineLong" + first +
			" mutates=true hold=false",
		"verbComposeBuild name=composeBuild program=programCompose prefix=[compose -p] suffix=[-f - build] alt=[]" +
			user + " deadline=deadlineLong stderr=stderrCount mutates=true hold=false",
		"verbImport name=import" + docker + " prefix=[import] suffix=[-] alt=[]" + made + " deadline=deadlineCopy" +
			first + held,
		"verbCommit name=commit" + docker + " prefix=[commit] suffix=[] alt=[]" + made + " deadline=deadlineCopy" +
			first + held,
		"verbCreate name=create" + docker + " prefix=[create --pull never] suffix=[] alt=[]" + made + short + first +
			held,
		"verbCopyIn name=copyIn" + docker + " prefix=[cp -] suffix=[] alt=[]" + made + " deadline=deadlineCopy" +
			first + held,
		"verbCopyOut name=copyOut" + docker + " prefix=[cp] suffix=[-] alt=[]" + made + " deadline=deadlineCopy" +
			first + read,
		"verbStart name=start" + docker + " prefix=[start] suffix=[] alt=[]" + made + short + first +
			" mutates=true hold=false",
		"verbStopGraceful name=stopGraceful" + docker + " prefix=[stop] suffix=[] alt=[]" + made +
			" deadline=deadlineGrace" + first + held,
		"verbWait name=wait" + docker + " prefix=[wait] suffix=[] alt=[]" + made + " deadline=deadlineNone" + first +
			read,
		"verbLogs name=logs" + docker + " prefix=[logs] suffix=[] alt=[]" + made + " deadline=deadlineCopy" +
			" stderr=stderrOutput" + read,
		"verbLogsFollow name=logsFollow" + docker + " prefix=[logs --follow] suffix=[] alt=[]" + made +
			" deadline=deadlineNone" + first + read,
	}
}

// findings are what one detector saw, and what of it breaks the audit.
type findings struct {
	seen       []string
	violations []string
}

// goSource is one parsed Go file.
type goSource struct {
	file *ast.File
	path string
}

// textSource is one tracked non-Go file the audit reads.
type textSource struct {
	path string
	text string
}

// auditSets are the tracked files, by set.
type auditSets struct {
	goFiles   []goSource
	workflows []textSource
	makefiles []textSource
	scripts   []textSource
}

// moduleRoot walks up from the working directory to go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the working directory")
		}

		dir = parent
	}
}

// loadAuditSets reads every tracked file the audit scans. Without git the audit fails; it never
// skips.
func loadAuditSets(t *testing.T) auditSets {
	t.Helper()

	root := moduleRoot(t)

	out, err := exec.CommandContext(t.Context(), "git", "-C", root, "ls-files", "-z").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}

	var sets auditSets

	for path := range strings.SplitSeq(strings.TrimSuffix(string(out), "\x00"), "\x00") {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}

		sets.add(t, path, data)
	}

	return sets
}

func (s *auditSets) add(t *testing.T, path string, data []byte) {
	t.Helper()

	workflow := regexp.MustCompile(`^\.github/workflows/[^/]+\.ya?ml$`)

	switch {
	case strings.HasSuffix(path, ".go"):
		s.goFiles = append(s.goFiles, parseGo(t, path, data))
	case workflow.MatchString(path):
		s.workflows = append(s.workflows, textSource{path: path, text: string(data)})
	case filepath.Base(path) == "Makefile":
		s.makefiles = append(s.makefiles, textSource{path: path, text: string(data)})
	case strings.HasSuffix(path, ".sh") || strings.HasSuffix(path, ".bash") || bytes.HasPrefix(data, []byte("#!")):
		s.scripts = append(s.scripts, textSource{path: path, text: string(data)})
	default:
	}
}

func (s *auditSets) texts() []textSource {
	return slices.Concat(s.workflows, s.makefiles, s.scripts)
}

func parseGo(t *testing.T, path string, src []byte) goSource {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	return goSource{path: path, file: file}
}

// production reports a non-test Go file.
func production(path string) bool {
	return !strings.HasSuffix(path, "_test.go")
}

// product reports a file of the product itself: not a test, and not the test helpers, which no
// product file may import.
func product(path string) bool {
	return production(path) && !strings.HasPrefix(path, "internal/dockertest/")
}

// provisionFile reports a file of the provision package itself.
func provisionFile(path string) bool {
	return filepath.Dir(path) == "internal/provision"
}

// stringLiterals returns every string literal in a file, unquoted.
func stringLiterals(file *ast.File) []string {
	var out []string

	ast.Inspect(file, func(node ast.Node) bool {
		if lit, ok := node.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			if text, err := strconv.Unquote(lit.Value); err == nil {
				out = append(out, text)
			}
		}

		return true
	})

	return out
}

// spawners finds every file that can start a process: importing os/exec, or calling
// os.StartProcess, syscall.ForkExec, syscall.Exec or syscall.StartProcess. Only allowlisted paths
// may; and exec.Command appears in production only in the runner.
func spawners(files []goSource) findings {
	var found, violations []string

	for _, f := range files {
		if !spawns(f.file) {
			continue
		}

		found = append(found, f.path)

		if _, allowed := spawnersAllowed()[f.path]; !allowed {
			violations = append(violations, "item 1: "+f.path+" can start a process and is not allowlisted")
		}

		if product(f.path) && f.path != runnerFile && callsSelector(f.file, "exec", "Command", "CommandContext") {
			violations = append(violations, "item 2: "+f.path+" calls exec.Command outside the runner")
		}
	}

	return findings{seen: found, violations: violations}
}

func spawns(file *ast.File) bool {
	for _, spec := range file.Imports {
		if spec.Path.Value == `"os/exec"` {
			return true
		}
	}

	return callsSelector(file, "os", "StartProcess") ||
		callsSelector(file, "syscall", "ForkExec", "Exec", "StartProcess")
}

// callsSelector reports a call of pkg.Name for any of names.
func callsSelector(file *ast.File, pkg string, names ...string) bool {
	found := false

	ast.Inspect(file, func(node ast.Node) bool {
		sel, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == pkg && slices.Contains(names, sel.Sel.Name) {
			found = true
		}

		return true
	})

	return found
}

// verbRows renders every row of runner.go's verbs() literal in the audit's canonical form.
func verbRows(files []goSource) []string {
	var rows []string

	for _, f := range files {
		if f.path != runnerFile {
			continue
		}

		for _, decl := range f.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Name.Name == "verbs" && fn.Body != nil {
				rows = append(rows, tableRows(fn)...)
			}
		}
	}

	return rows
}

func tableRows(fn *ast.FuncDecl) []string {
	var rows []string

	ast.Inspect(fn.Body, func(node ast.Node) bool {
		kv, ok := node.(*ast.KeyValueExpr)
		if !ok {
			return true
		}

		key, isIdent := kv.Key.(*ast.Ident)
		row, isRow := kv.Value.(*ast.CompositeLit)

		if isIdent && isRow && strings.HasPrefix(key.Name, "verb") {
			rows = append(rows, renderRow(key.Name, row))

			return false
		}

		return true
	})

	return rows
}

func renderRow(verb string, row *ast.CompositeLit) string {
	fields := map[string]string{
		"suffix": "[]", "alt": "[]", "mutates": "false", "hold": "false", "prefix": "[]",
	}

	for _, elt := range row.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		if key, ok := kv.Key.(*ast.Ident); ok {
			fields[key.Name] = renderValue(kv.Value)
		}
	}

	var out strings.Builder

	out.WriteString(verb)

	keys := []string{"name", "program", "prefix", "suffix", "alt", "mode", "deadline", "stderr", "mutates", "hold"}
	for _, key := range keys {
		out.WriteString(" " + key + "=" + fields[key])
	}

	return out.String()
}

func renderValue(expr ast.Expr) string {
	switch v := expr.(type) {
	case *ast.BasicLit:
		if text, err := strconv.Unquote(v.Value); err == nil {
			return text
		}

		return v.Value
	case *ast.Ident:
		return v.Name
	case *ast.CompositeLit:
		words := make([]string, 0, len(v.Elts))
		for _, elt := range v.Elts {
			words = append(words, renderValue(elt))
		}

		return "[" + strings.Join(words, " ") + "]"
	default:
		return "?"
	}
}

// mutatingVerbs are the constants of the rows that change the engine.
func mutatingVerbs(rows []string) []string {
	var out []string

	for _, row := range rows {
		if strings.Contains(row, " mutates=true ") {
			out = append(out, strings.Fields(row)[0])
		}
	}

	return out
}

// mutatorReferences finds a mutating verb referenced by production code other than the owner.
func mutatorReferences(files []goSource, mutating []string) []string {
	var violations []string

	for _, f := range files {
		if !production(f.path) || f.path == ownerFile || f.path == runnerFile || !provisionFile(f.path) {
			continue
		}

		ast.Inspect(f.file, func(node ast.Node) bool {
			if ident, ok := node.(*ast.Ident); ok && slices.Contains(mutating, ident.Name) {
				violations = append(violations, "item 2: "+f.path+" issues "+ident.Name+" outside the owner")
			}

			return true
		})
	}

	return violations
}

// composeSubcommands are compose's subcommands other than the two the runner may issue.
func composeSubcommands() []string {
	return []string{
		"attach", "commit", "cp", "create", "down", "events", "exec", "export", "images", "kill", "logs", "ls",
		"pause", "port", "ps", "publish", "pull", "push", "restart", "rm", "run", "scale", "start", "stats", "stop",
		"top", "unpause", "up", "version", "volumes", "wait", "watch",
	}
}

// composeSites finds every argv-shaped string list naming compose: one that also names a compose
// subcommand other than config or build is a violation anywhere; the runner's two compose rows are
// checked apart, exactly.
func composeSites(files []goSource) findings {
	var sites, violations []string

	for _, f := range files {
		ast.Inspect(f.file, func(node ast.Node) bool {
			lit, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}

			words := literalWords(lit)
			if !slices.Contains(words, "compose") {
				return true
			}

			sites = append(sites, f.path+": "+strings.Join(words, " "))

			if f.path != auditFile && slices.ContainsFunc(words, func(w string) bool {
				return slices.Contains(composeSubcommands(), w)
			}) {
				violations = append(violations, "item 3: "+f.path+" names compose "+strings.Join(words, " "))
			}

			return true
		})
	}

	return findings{seen: sites, violations: violations}
}

// literalWords returns a composite literal's string elements.
func literalWords(lit *ast.CompositeLit) []string {
	var words []string

	for _, elt := range lit.Elts {
		if basic, ok := elt.(*ast.BasicLit); ok && basic.Kind == token.STRING {
			if text, err := strconv.Unquote(basic.Value); err == nil {
				words = append(words, text)
			}
		}
	}

	return words
}

// composeRows checks the runner's compose rows are exactly its two.
func composeRows(rows []string) []string {
	want := []string{
		"prefix=[compose] suffix=[config --format json] alt=[config --environment]",
		"prefix=[compose -p] suffix=[-f - build] alt=[]",
	}

	var got []string

	for _, row := range rows {
		if strings.Contains(row, "program=programCompose") {
			start := strings.Index(row, "prefix=")
			end := strings.Index(row, " mode=")
			got = append(got, row[start:end])
		}
	}

	if !slices.Equal(got, want) {
		return []string{"item 3: the runner's compose rows are " + strings.Join(got, "; ")}
	}

	return nil
}

var (
	pruneAPIPath  = regexp.MustCompile(`(^|/)prune($|[/?])`)
	dockerCommand = regexp.MustCompile(`\bdocker\s+([a-z][a-z0-9-]*)`)
	pruneToken    = regexp.MustCompile(`(^|[^\w-])prune\b`)
	// unselectedPort is a publish whose host port Stutter did not select: a constant one
	// ("127.0.0.1:8080:", "8080:80/tcp") or one left to the engine ("127.0.0.1::").
	unselectedPort = regexp.MustCompile(`\d{1,3}(\.\d{1,3}){3}:\d{0,5}:|^\d{1,5}:\d{1,5}(/(tcp|udp|sctp))?$`)
	// composeRun is the compose plugin given an argument: run, not a file named by a download,
	// checksum or move.
	composeRun = regexp.MustCompile(`docker-compose"?[ \t]+[^\s|;&<>\\"'` + "`" + `)]`)
)

// pruneInGo finds prune as an argv element, an API path, or a docker command string.
func pruneInGo(files []goSource) []string {
	var violations []string

	for _, f := range files {
		if pruneExempt()[f.path] {
			continue
		}

		for _, text := range stringLiterals(f.file) {
			if text == "prune" || pruneAPIPath.MatchString(text) ||
				strings.Contains(text, "docker") && pruneToken.MatchString(strings.ReplaceAll(text, "--no-prune", "")) {
				violations = append(violations, "item 4: "+f.path+" names prune in "+strconv.Quote(text))
			}
		}
	}

	return violations
}

// textInvocations checks workflows, the Makefile and scripts: docker only as `docker version` or
// `docker info`, never compose, never prune.
func textInvocations(texts []textSource) []string {
	var violations []string

	for _, src := range texts {
		for line := range strings.Lines(src.text) {
			for _, match := range dockerCommand.FindAllStringSubmatch(line, -1) {
				if match[1] != "version" && match[1] != "info" {
					violations = append(violations, "item 5: "+src.path+" runs docker "+match[1])
				}
			}

			if composeRun.MatchString(line) {
				violations = append(violations, "item 3: "+src.path+" runs docker-compose")
			}

			if strings.Contains(line, "docker") && pruneToken.MatchString(strings.ReplaceAll(line, "--no-prune", "")) {
				violations = append(violations, "item 4: "+src.path+" names prune after docker")
			}
		}
	}

	return violations
}

// ipamFlags finds a production literal that would let the engine pick an address, or that publishes
// on a host port Stutter did not select; --subnet is the caller's, set once, in the network builder.
func ipamFlags(files []goSource) []string {
	var violations []string

	subnets := 0

	for _, f := range files {
		if !product(f.path) {
			continue
		}

		for _, text := range stringLiterals(f.file) {
			switch {
			case slices.Contains([]string{"--ip", "--ip6", "--ip-range", "--gateway"}, text),
				strings.HasPrefix(text, "--ipv6") && text != "--ipv6=false":
				violations = append(violations, "item 5a: "+f.path+" carries "+text)
			case unselectedPort.MatchString(text):
				violations = append(violations, "item 5b: "+f.path+" publishes on a host port it did not select: "+text)
			case text == "--subnet":
				subnets++

				if f.path != argvFile {
					violations = append(violations, "item 5a: "+f.path+" carries --subnet")
				}
			default:
			}
		}
	}

	if subnets != 1 {
		violations = append(violations, "item 5a: --subnet appears "+strconv.Itoa(subnets)+" times, not once")
	}

	return violations
}

// handleTypes are the types a removal, stop, start, copy or commit may take.
func handleTypes() []string {
	return []string{"*Container", "*Network", "*Volume", "Handle"}
}

// mutatorPrefixes name the exported functions that act on an engine resource.
func mutatorPrefixes() []string {
	return []string{"Remove", "Stop", "Start", "Connect", "Copy", "Commit", "Retire", "Kill"}
}

// surface checks the provision package's exported surface: every mutator takes a handle first,
// never a resource's ID or name; handles have no exported field or constructor.
func surface(files []goSource) findings {
	var mutators, violations []string

	for _, f := range files {
		if !production(f.path) || !provisionFile(f.path) {
			continue
		}

		for _, decl := range f.file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				checked := checkFunc(f.path, d)
				mutators, violations = append(mutators, checked.seen...), append(violations, checked.violations...)
			case *ast.GenDecl:
				violations = append(violations, checkTypes(f.path, d)...)
			default:
			}
		}
	}

	return findings{seen: mutators, violations: violations}
}

func checkFunc(path string, fn *ast.FuncDecl) findings {
	if !fn.Name.IsExported() {
		return findings{}
	}

	var out findings

	if fn.Recv == nil && fn.Type.Results != nil {
		for _, result := range fn.Type.Results.List {
			if slices.Contains(handleTypes()[:3], typeString(result.Type)) {
				out.violations = append(out.violations, "item 6: "+path+" "+fn.Name.Name+" constructs a handle")
			}
		}
	}

	if !slices.ContainsFunc(mutatorPrefixes(), func(p string) bool { return strings.HasPrefix(fn.Name.Name, p) }) {
		return out
	}

	out.seen = []string{fn.Name.Name}

	params := flatParams(fn.Type.Params)
	if len(params) < 2 || !slices.Contains(handleTypes(), params[1].typ) {
		out.violations = append(out.violations, "item 6: "+path+" "+fn.Name.Name+" does not take a handle first")
	}

	for _, p := range params {
		if p.typ == "string" && namesResource(p.name) {
			out.violations = append(out.violations, "item 6: "+path+" "+fn.Name.Name+" takes a resource by "+p.name)
		}
	}

	return out
}

type param struct{ name, typ string }

func flatParams(list *ast.FieldList) []param {
	var out []param

	for _, field := range list.List {
		typ := typeString(field.Type)
		if len(field.Names) == 0 {
			out = append(out, param{typ: typ})
		}

		for _, name := range field.Names {
			out = append(out, param{name: name.Name, typ: typ})
		}
	}

	return out
}

func namesResource(name string) bool {
	banned := []string{"id", "name", "ref", "reference", "container", "network", "volume", "image"}

	return slices.Contains(banned, name) || strings.HasSuffix(name, "ID") || strings.HasSuffix(name, "Name") ||
		strings.HasSuffix(name, "Ref")
}

func typeString(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return "*" + typeString(t.X)
	case *ast.SelectorExpr:
		return typeString(t.X) + "." + t.Sel.Name
	default:
		return "?"
	}
}

// checkTypes refuses an exported field on a handle, and a Handle whose methods are all exported.
func checkTypes(path string, decl *ast.GenDecl) []string {
	var violations []string

	for _, spec := range decl.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}

		switch body := ts.Type.(type) {
		case *ast.StructType:
			if slices.Contains([]string{"Container", "Network", "Volume"}, ts.Name.Name) &&
				slices.ContainsFunc(body.Fields.List, exportedField) {
				violations = append(violations, "item 6: "+path+" "+ts.Name.Name+" has an exported field")
			}
		case *ast.InterfaceType:
			if ts.Name.Name == "Handle" && !slices.ContainsFunc(body.Methods.List, unexportedMethod) {
				violations = append(violations, "item 6: "+path+" Handle has no unexported method")
			}
		default:
		}
	}

	return violations
}

func exportedField(field *ast.Field) bool {
	return slices.ContainsFunc(field.Names, func(n *ast.Ident) bool { return n.IsExported() })
}

func unexportedMethod(field *ast.Field) bool {
	return len(field.Names) == 1 && !field.Names[0].IsExported()
}

// decoyVerbs reads the verb list of the test-decoy helper, when it exists: it may hold no prune and
// no compose subcommand.
func decoyVerbs(files []goSource) (findings, bool) {
	for _, f := range files {
		if f.path != decoyFile {
			continue
		}

		var verbs, violations []string

		for _, decl := range f.file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "decoyVerbs" && fn.Body != nil {
				verbs = append(verbs, stringLiterals(&ast.File{Decls: []ast.Decl{fn}})...)
			}
		}

		for _, verb := range verbs {
			if verb == "prune" || verb == "compose" {
				violations = append(violations, "item 2b: the decoy helper admits "+verb)
			}
		}

		if len(verbs) == 0 {
			violations = append(violations, "item 2b: the decoy helper's decoyVerbs lists nothing")
		}

		return findings{seen: verbs, violations: violations}, true
	}

	return findings{}, false
}

// AUDIT-1 holds on the tracked tree: only the runner spawns in production; its verb table is the
// admitted one; only the owner issues a mutating verb; compose is called only as config and build;
// nothing names prune; CI runs docker only for version and info; no argv lets the engine pick an
// address; and nothing exported removes, stops or copies by a resource's name.
func TestAuditOneHolds(t *testing.T) {
	t.Parallel()

	sets := loadAuditSets(t)
	rows := verbRows(sets.goFiles)
	spawned, sites, exported := spawners(sets.goFiles), composeSites(sets.goFiles), surface(sets.goFiles)
	importers, mutators := spawned.seen, exported.seen
	violations := slices.Concat(spawned.violations, sites.violations, exported.violations)

	if !slices.Equal(rows, expectedVerbs()) {
		violations = append(violations, "item 2: the runner's verb table differs from the admitted one")

		diffRows(t, rows, expectedVerbs())
	}

	violations = slices.Concat(violations, composeRows(rows),
		mutatorReferences(sets.goFiles, mutatingVerbs(rows)), pruneInGo(sets.goFiles), textInvocations(sets.texts()),
		ipamFlags(sets.goFiles))

	decoys, decoyPresent := decoyVerbs(sets.goFiles)
	violations = append(violations, decoys.violations...)

	if decoyPresent {
		t.Logf("decoy verbs=%d %v", len(decoys.seen), decoys.seen)
	} else {
		t.Logf("decoy helper %s is absent", decoyFile)
	}

	logAudit(t, sets, importers, rows, sites.seen, mutators)

	t.Logf("STUTTER-AUDIT static files-go=%d files-workflow=%d files-make=%d files-script=%d spawners=%d verbs=%d"+
		" compose-sites=%d mutators=%d", len(sets.goFiles), len(sets.workflows), len(sets.makefiles), len(sets.scripts),
		len(importers), len(rows), len(sites.seen), len(mutators))

	for name, count := range map[string]int{
		"files-go": len(sets.goFiles), "files-workflow": len(sets.workflows), "spawners": len(importers),
		"verbs": len(rows), "mutators": len(mutators),
	} {
		if count == 0 {
			t.Errorf("%s is 0: the audit saw nothing to judge", name)
		}
	}

	for _, v := range violations {
		t.Error(v)
	}
}

func logAudit(t *testing.T, sets auditSets, importers, rows, sites, mutators []string) {
	t.Helper()

	present := map[string]bool{}
	for _, f := range sets.goFiles {
		present[f.path] = true
	}

	for path, why := range spawnersAllowed() {
		if !present[path] {
			t.Logf("allowlisted spawner %s is absent", path)
		} else if slices.Contains(importers, path) {
			t.Logf("spawner %s: %s", path, why)
		}
	}

	for _, row := range rows {
		t.Logf("verb %s", row)
	}

	for _, site := range sites {
		t.Logf("compose site %s", site)
	}

	for _, mutator := range mutators {
		t.Logf("mutator %s", mutator)
	}
}

func diffRows(t *testing.T, got, want []string) {
	t.Helper()

	for _, row := range got {
		if !slices.Contains(want, row) {
			t.Logf("not admitted: %s", row)
		}
	}

	for _, row := range want {
		if !slices.Contains(got, row) {
			t.Logf("admitted but absent: %s", row)
		}
	}
}

// Every detector catches the break it exists for, over files made for the purpose.
// itemOf keeps the violations of one audit item.
func itemOf(item string, violations []string) []string {
	var kept []string

	for _, v := range violations {
		if strings.HasPrefix(v, item+":") {
			kept = append(kept, v)
		}
	}

	return kept
}

func TestAuditOneCatchesEachBreak(t *testing.T) {
	t.Parallel()

	root := moduleRoot(t)

	runner, err := os.ReadFile(filepath.Join(root, runnerFile))
	if err != nil {
		t.Fatal(err)
	}

	extraRow := strings.Replace(string(runner), "\t\tverbContext: {", "\t\tverbExtra: {name: \"exec\", "+
		"program: programDocker, prefix: []string{\"exec\"}},\n\t\tverbContext: {", 1)

	breaks := []struct {
		detect func() []string
		name   string
	}{
		{name: "a second production os/exec importer", detect: func() []string {
			src := "package provision\n\nimport _ \"os/exec\"\n"

			return spawners([]goSource{parseGo(t, ownerFile, []byte(src))}).violations
		}},
		{name: "a prune argv token", detect: func() []string {
			src := "package provision\n\nvar x = []string{\"prune\"}\n"

			return pruneInGo([]goSource{parseGo(t, argvFile, []byte(src))})
		}},
		{name: "compose up", detect: func() []string {
			src := "package provision_test\n\nvar x = []string{\"compose\", \"up\"}\n"
			return composeSites([]goSource{parseGo(t, "internal/provision/x_test.go", []byte(src))}).violations
		}},
		{name: "a Makefile running the compose plugin", detect: func() []string {
			return textInvocations([]textSource{{path: "Makefile", text: "\t\"$(DEST)/docker-compose\" up -d\n"}})
		}},
		{name: "a verb added to the runner only", detect: func() []string {
			rows := verbRows([]goSource{parseGo(t, runnerFile, []byte(extraRow))})
			if slices.Equal(rows, expectedVerbs()) {
				return nil
			}

			return []string{"table differs"}
		}},
		{name: "a constant host port", detect: func() []string {
			src := "package provision\n\nvar x = []string{\"-p\", \"127.0.0.1:8080:80/tcp\"}\n"

			return itemOf("item 5b", ipamFlags([]goSource{parseGo(t, argvFile, []byte(src))}))
		}},
		{name: "a host port left to the engine", detect: func() []string {
			src := "package provision\n\nvar x = \"127.0.0.1::\" + \"80/tcp\"\n"

			return itemOf("item 5b", ipamFlags([]goSource{parseGo(t, argvFile, []byte(src))}))
		}},
		{name: "an exported RemoveByID(id string)", detect: func() []string {
			src := "package provision\n\nimport \"context\"\n\n" +
				"func (e *Engine) RemoveByID(ctx context.Context, id string) error { return nil }\n"

			return surface([]goSource{parseGo(t, "internal/provision/extra.go", []byte(src))}).violations
		}},
	}

	caught := 0

	for _, b := range breaks {
		if found := b.detect(); len(found) > 0 {
			caught++

			t.Logf("%s: %v", b.name, found)
		} else {
			t.Errorf("%s was not caught", b.name)
		}
	}

	t.Logf("breaks=%d caught=%d", len(breaks), caught)
}
