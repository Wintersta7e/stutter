package provision

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// imageTemplate reads what the compose model needs of an image. An image's inspect fails outright on
// a key its JSON lacks, so every key that can be absent is read with index.
const imageTemplate = `{"id":{{json .Id}},"labels":{{json (index .Config "Labels")}},` +
	`"env":{{json (index .Config "Env")}},"entrypoint":{{json (index .Config "Entrypoint")}},` +
	`"cmd":{{json (index .Config "Cmd")}},"exposed":{{json (index .Config "ExposedPorts")}},` +
	`"volumes":{{json (index .Config "Volumes")}},"os":{{json (index . "Os")}},` +
	`"working_dir":{{json (index .Config "WorkingDir")}},"user":{{json (index .Config "User")}},` +
	`"arch":{{json (index . "Architecture")}},"variant":{{json (index . "Variant")}}}`

// imageReport is what imageTemplate prints.
type imageReport struct {
	Labels     map[string]string          `json:"labels"`
	Exposed    map[string]json.RawMessage `json:"exposed"`
	Volumes    map[string]json.RawMessage `json:"volumes"`
	ID         string                     `json:"id"`
	OS         string                     `json:"os"`
	Arch       string                     `json:"arch"`
	Variant    string                     `json:"variant"`
	WorkingDir string                     `json:"working_dir"`
	User       string                     `json:"user"`
	Env        []string                   `json:"env"`
	Entrypoint []string                   `json:"entrypoint"`
	Cmd        []string                   `json:"cmd"`
}

// image converts a report to the compose model's image.
func (r imageReport) image() compose.Image {
	return compose.Image{
		Labels: r.Labels, ID: r.ID, OS: r.OS, Arch: r.Arch, Variant: r.Variant, WorkingDir: r.WorkingDir,
		User: r.User, Env: r.Env,
		Entrypoint: r.Entrypoint, Cmd: r.Cmd, ExposedPorts: sortedKeys(r.Exposed), Volumes: sortedKeys(r.Volumes),
	}
}

func sortedKeys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}

	slices.Sort(out)

	return out
}

// inspectImage reads an image by reference or ID; not found on an engine that answers is absent.
func (e *Engine) inspectImage(ctx context.Context, ref string) (compose.Image, bool, error) {
	var report imageReport

	found, err := e.read(ctx, request{verb: verbImageInspect, args: []arg{{val: imageTemplate}, {val: ref}}}, &report)
	if err != nil || !found {
		return compose.Image{}, found, err
	}

	return report.image(), true, nil
}

// LocalImage reads the image ref names as the engine holds it now, and neither pulls nor pins it: what
// classification reads before anything is resolved, and how `pull_policy: never` learns an image is
// absent. Not found on an engine that answers is absent; an engine that does not answer is an error.
func (e *Engine) LocalImage(ctx context.Context, ref string) (compose.Image, bool, error) {
	return e.inspectImage(ctx, ref)
}

// ResolveImage pins the image ref names: present, its ID is pinned; absent — told apart from an
// engine that does not answer — it is pulled once, in the user's environment, then pinned. A
// reference already resolved returns what was pinned, whatever it names now. Nothing is resolved
// after the first container. A platform other than the engine's is recorded in the image, never
// refused.
func (e *Engine) ResolveImage(ctx context.Context, ref, platform string) (compose.Image, error) {
	if image, ok := e.pinnedRef(ref); ok {
		return image, nil
	}

	if e.book.containerCreated() {
		return compose.Image{}, fmt.Errorf("%w: %s asked for after the first container", ErrImage, ref)
	}

	image, found, err := e.inspectImage(ctx, ref)
	if err != nil {
		return compose.Image{}, err
	}

	if !found {
		if pullErr := e.pull(ctx, ref, platform); pullErr != nil {
			return compose.Image{}, fmt.Errorf("%w: pull %s: %w", ErrImage, ref, pullErr)
		}

		if image, found, err = e.inspectImage(ctx, ref); err != nil || !found {
			return compose.Image{}, fmt.Errorf("%w: %s is absent after its pull: %w", ErrImage, ref, err)
		}
	}

	e.pin(ref, image)

	return image, nil
}

// pin records an image the check will run, by reference and by ID.
func (e *Engine) pin(ref string, image compose.Image) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.pins == nil {
		e.pins = map[string]compose.Image{}
	}

	e.pins[ref] = image
}

// pinnedRef returns the image ref was pinned to.
func (e *Engine) pinnedRef(ref string) (compose.Image, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	image, ok := e.pins[ref]

	return image, ok
}

// pinnedImage returns the image this check pinned under id.
func (e *Engine) pinnedImage(id string) (compose.Image, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, image := range e.pins {
		if image.ID == id && id != "" {
			return image, true
		}
	}

	return compose.Image{}, false
}
