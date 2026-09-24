package compose

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// Parse reads the compose project once for the check: the whole project with its active profiles,
// never one service's `depends_on` closure. A service absent from it is looked up narrowly, only to
// learn the profiles that activate it. Compose's own stderr and error text are never kept: they can
// echo interpolated values, so a failure names the step, the exit status and the command to
// reproduce it.
func Parse(ctx context.Context, run ConfigFunc, in Inputs) (*Model, error) {
	if err := in.check(); err != nil {
		return nil, err
	}

	p := parser{run: run, dir: filepath.Dir(in.Files[0]), files: in.Files, profiles: slices.Clone(in.Profiles)}

	model, err := p.whole(ctx)
	if err != nil {
		return nil, err
	}

	if _, ok := model.typed.Services[in.Service]; !ok {
		if model, err = p.activate(ctx, in.Service); err != nil {
			return nil, err
		}
	}

	if err = p.environment(ctx, model); err != nil {
		return nil, err
	}

	keys, present, err := dotEnvKeys(p.dir)
	if err != nil {
		return nil, err
	}

	model.service = in.Service
	model.dotEnv = present
	model.composeVars = composeVars(os.Environ(), keys)

	return model, nil
}

func (in Inputs) check() error {
	if in.Service == "" {
		return fmt.Errorf("%w: no service named", ErrModel)
	}

	if len(in.Files) == 0 {
		return fmt.Errorf("%w: no compose file named", ErrModel)
	}

	for _, file := range in.Files {
		if !filepath.IsAbs(file) {
			return fmt.Errorf("%w: compose file %s is not an absolute path", ErrModel, file)
		}
	}

	return nil
}

// parser runs the compose reads of one Parse.
type parser struct {
	run      ConfigFunc
	dir      string
	files    []string
	profiles []string
}

// args are the compose arguments every read shares: files in order, then profiles.
func (p *parser) args() []string {
	const wordsPerFlag = 2 // `-f FILE` and `--profile NAME`

	out := make([]string, 0, wordsPerFlag*(len(p.files)+len(p.profiles)))

	for _, file := range p.files {
		out = append(out, "-f", file)
	}

	for _, profile := range p.profiles {
		out = append(out, "--profile", profile)
	}

	return out
}

// command renders a compose read for a user to run: files and flags only.
func (p *parser) command(tail ...string) string {
	words := append([]string{"docker", "compose"}, p.args()...)
	words = append(words, "config")
	words = append(words, tail...)

	for index, word := range words {
		words[index] = shellWord(word)
	}

	return strings.Join(words, " ")
}

// whole reads and decodes the whole project.
func (p *parser) whole(ctx context.Context) (*Model, error) {
	command := p.command("--format", "json")

	out, lines, err := p.run(ctx, p.dir, p.args(), ConfigRead{})
	if err != nil {
		return nil, failure(ctx, "parse", command, err)
	}

	root, typed, err := decodeModel(out, command)
	if err != nil {
		return nil, err
	}

	return &Model{
		root: root, typed: typed, reproduce: command, files: slices.Clone(p.files), stderrLines: lines,
	}, nil
}

// activate learns the profiles that gate service and reads the whole project again with them.
func (p *parser) activate(ctx context.Context, service string) (*Model, error) {
	narrow := p.command("--format", "json", service)
	absent := fmt.Errorf("%w: service %s is not in the compose model; reproduce with: %s", ErrModel, service, narrow)

	out, _, err := p.run(ctx, p.dir, p.args(), ConfigRead{Service: service})
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("compose parse: %w", ctx.Err())
		}

		return nil, absent
	}

	var gated struct {
		Services map[string]struct {
			Profiles []string `json:"profiles"`
		} `json:"services"`
	}

	if json.Unmarshal(out, &gated) != nil || len(gated.Services[service].Profiles) == 0 {
		return nil, absent
	}

	p.profiles = append(p.profiles, gated.Services[service].Profiles...)

	model, err := p.whole(ctx)
	if err != nil {
		return nil, err
	}

	if _, ok := model.typed.Services[service]; !ok {
		return nil, fmt.Errorf("%w: service %s is not in the compose model; reproduce with: %s",
			ErrModel, service, model.reproduce)
	}

	return model, nil
}

// environment reads the value of every variable an `environment:` config or secret of some service
// names. Compose leaves those unresolved in its JSON, so its resolved environment is read, and
// everything else in it is discarded at once.
func (p *parser) environment(ctx context.Context, model *Model) error {
	model.sourced = sourcedVariables(model.typed)
	if len(model.sourced) == 0 {
		return nil
	}

	out, _, err := p.run(ctx, p.dir, p.args(), ConfigRead{Environment: true})
	if err != nil {
		return failure(ctx, "environment read", p.command("--environment"), err)
	}

	model.environ = environmentValues(out, model.sourced)

	return nil
}

// sourcedVariables names every variable an `environment:` config or secret that a service uses
// reads: sorted, unique.
func sourcedVariables(typed composeProject) []string {
	var names []string

	for _, svc := range typed.Services {
		for _, ref := range svc.Configs {
			if def := typed.Configs[ref.Source]; def != nil && def.Environment != "" {
				names = append(names, def.Environment)
			}
		}

		for _, ref := range svc.Secrets {
			if def := typed.Secrets[ref.Source]; def != nil && def.Environment != "" {
				names = append(names, def.Environment)
			}
		}
	}

	slices.Sort(names)

	return slices.Compact(names)
}

// environmentKey starts a `KEY=VALUE` line of compose's resolved environment. Any other line
// continues the previous value.
var environmentKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=`)

// environmentValues keeps the values of names from compose's `config --environment` output.
func environmentValues(out []byte, names []string) map[string]string {
	values := map[string]string{}
	current := ""

	for line := range strings.Lines(strings.TrimSuffix(string(out), "\n")) {
		line = strings.TrimSuffix(line, "\n")

		if environmentKey.MatchString(line) {
			key, value, _ := strings.Cut(line, "=")
			current = key

			if slices.Contains(names, key) {
				values[key] = value
			}

			continue
		}

		if _, kept := values[current]; kept {
			values[current] += "\n" + line
		}
	}

	return values
}

// decodeModel decodes compose's JSON twice: generically, numbers kept verbatim, for the closed walk
// and the build stdin; and into the typed form every later step reads.
func decodeModel(out []byte, command string) (map[string]any, composeProject, error) {
	decoder := json.NewDecoder(bytes.NewReader(out))
	decoder.UseNumber()

	var root map[string]any
	if err := decoder.Decode(&root); err != nil {
		return nil, composeProject{}, shapeError(err, command)
	}

	if err := walkModel(root); err != nil {
		return nil, composeProject{}, err
	}

	var typed composeProject
	if err := json.Unmarshal(out, &typed); err != nil {
		return nil, composeProject{}, shapeError(err, command)
	}

	for name, svc := range typed.Services {
		if svc == nil {
			typed.Services[name] = &composeService{}
		}
	}

	return root, typed, nil
}

// shapeError reports a model compose printed that Stutter cannot read, naming at most the key.
func shapeError(err error, command string) error {
	where := ""

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) && typeErr.Field != "" {
		where = " at " + typeErr.Field
	}

	return fmt.Errorf("%w: compose printed a model Stutter cannot read%s; reproduce with: %s", ErrModel, where, command)
}

// failure reports a compose read that failed, never with compose's own text.
func failure(ctx context.Context, step, command string, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("compose %s: %w", step, ctx.Err())
	}

	var exit interface{ ExitCode() int }
	if errors.As(err, &exit) {
		return fmt.Errorf("%w: compose %s failed with exit status %d; reproduce with: %s",
			ErrModel, step, exit.ExitCode(), command)
	}

	return fmt.Errorf("%w: compose %s could not run; reproduce with: %s", ErrModel, step, command)
}

// shellWord quotes a word for a POSIX shell when it needs it.
func shellWord(word string) string {
	if word != "" && !strings.ContainsAny(word, " \t\n'\"\\$`;&|<>()*?[]#~!{}") {
		return word
	}

	return "'" + strings.ReplaceAll(word, "'", `'\''`) + "'"
}
