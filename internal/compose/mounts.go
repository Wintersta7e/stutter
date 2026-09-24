package compose

import (
	"fmt"
	"io/fs"
	"math"
	"path"
	"strconv"
	"strings"
)

// sizeUnit is the step between size suffixes: k, m, g and t are binary.
const sizeUnit = 1024

// defaultCopyMode is compose's mode for a copied-in config or secret that names none.
const defaultCopyMode fs.FileMode = 0o444

// applyMounts sets a spec's mounts and copies. A bind is always read-only; a named or anonymous
// volume is a fresh volume, never the user's own; every image VOLUME no mount covers gets a fresh
// volume too, so the engine never makes an unlabelled one; a `content:` or `environment:` config or
// secret is a file copied in, never a directory.
func (m *Model) applyMounts(spec *Spec, svc *composeService, img Image) error {
	for index, volume := range svc.Volumes {
		addVolume(spec, "volumes["+strconv.Itoa(index)+"]", volume)
	}

	for index, entry := range svc.Tmpfs {
		mount, err := shortTmpfs(entry)
		if err != nil {
			return fmt.Errorf("%w: service %s: tmpfs[%d] %w", ErrModel, spec.Service, index, err)
		}

		spec.Mounts = append(spec.Mounts, mount)
	}

	for _, file := range m.usedFiles(svc) {
		if err := m.addFile(spec, file); err != nil {
			return err
		}
	}

	covered := make(map[string]bool, len(spec.Mounts))
	for _, mount := range spec.Mounts {
		covered[path.Clean(mount.Target)] = true
	}

	for _, volume := range img.Volumes {
		if target := path.Clean(volume); !covered[target] {
			spec.Mounts = append(spec.Mounts, Mount{Kind: MountFresh, Target: target})
			covered[target] = true
		}
	}

	return nil
}

// addVolume adds one long-form `volumes` entry.
func addVolume(s *Spec, key string, volume serviceVolume) {
	target := unescape(volume.Target)

	switch volume.Type {
	case volumeTypeBind:
		s.Mounts = append(s.Mounts, Mount{
			Kind: MountBind, Source: unescape(volume.Source), Target: target, ReadOnly: true,
		})

		if !volume.ReadOnly {
			s.Replaced = append(s.Replaced, Replaced{Key: key, What: "mounted read-only"})
		}

		if volume.Bind != nil && volume.Bind.Recursive != "" {
			s.Replaced = append(s.Replaced, Replaced{
				Key: key + ".bind.recursive", What: "read-only including submounts",
			})
		}
	case volumeTypeVolume:
		mount := Mount{Kind: MountFresh, Target: target, Volume: volume.Source, ReadOnly: volume.ReadOnly}
		if volume.Volume != nil {
			mount.NoCopy = volume.Volume.NoCopy
		}

		s.Mounts = append(s.Mounts, mount)
		s.Replaced = append(s.Replaced, Replaced{Key: key, What: "a fresh volume at " + target})
	case volumeTypeTmpfs:
		mount := Mount{Kind: MountTmpfs, Target: target}
		if volume.Tmpfs != nil {
			mount.Size, mount.Mode = int64(volume.Tmpfs.Size), fs.FileMode(volume.Tmpfs.Mode)
		}

		s.Mounts = append(s.Mounts, mount)
	default:
	}
}

// shortTmpfs reads one `tmpfs` entry: a path, then optionally `:` and options, of which `size` and
// `mode` are kept.
func shortTmpfs(entry string) (Mount, error) {
	target, options, _ := strings.Cut(entry, ":")
	mount := Mount{Kind: MountTmpfs, Target: unescape(target)}

	for option := range strings.SplitSeq(options, ",") {
		name, value, _ := strings.Cut(option, "=")

		switch name {
		case "size":
			size, err := parseSize(value)
			if err != nil {
				return Mount{}, fmt.Errorf("has a size that is not a byte count: %w", errShape)
			}

			mount.Size = size
		case "mode":
			mode, err := strconv.ParseUint(value, 8, 32)
			if err != nil {
				return Mount{}, fmt.Errorf("has a mode that is not octal: %w", errShape)
			}

			mount.Mode = fs.FileMode(mode)
		default:
		}
	}

	return mount, nil
}

// parseSize reads a size in bytes, with an optional binary unit suffix: 64m, 1.5g, 512k, 100.
func parseSize(text string) (int64, error) {
	text = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(text)), "b")
	if text == "" {
		return 0, errShape
	}

	multiplier := 1.0

	if unit := strings.IndexByte("kmgt", text[len(text)-1]); unit >= 0 {
		multiplier = math.Pow(sizeUnit, float64(unit+1))
		text = text[:len(text)-1]
	}

	value, err := strconv.ParseFloat(text, 64)
	if err != nil || value < 0 {
		return 0, errShape
	}

	return int64(value * multiplier), nil
}

// addFile adds one config or secret: a `file:` source is bound read-only, a `content:` or
// `environment:` one is copied in with compose's ownership and mode.
func (m *Model) addFile(spec *Spec, file serviceFileRef) error {
	target := unescape(file.target)
	name := file.kind + "." + file.source

	switch {
	case file.def.File != "":
		spec.Mounts = append(spec.Mounts, Mount{
			Kind: MountBind, Source: unescape(file.def.File), Target: target, ReadOnly: true,
		})

		return nil
	case file.def.Content != nil:
		return addCopy(spec, name, target, file.use, []byte(unescape(*file.def.Content)))
	case file.def.Environment != "":
		value, ok := m.environ[file.def.Environment]
		if !ok {
			return fmt.Errorf("%w: service %s: %s reads variable %s, which is not set", ErrModel, spec.Service, name,
				file.def.Environment)
		}

		return addCopy(spec, name, target, file.use, []byte(value))
	default:
		return nil
	}
}

// addCopy adds one copied-in file.
func addCopy(s *Spec, name, target string, use serviceFile, data []byte) error {
	copyIn := CopyIn{Target: target, Mode: defaultCopyMode, data: data}

	for _, owner := range []struct {
		into *int
		key  string
		text string
	}{
		{into: &copyIn.UID, key: "uid", text: use.UID},
		{into: &copyIn.GID, key: "gid", text: use.GID},
	} {
		if owner.text == "" {
			continue
		}

		id, err := strconv.Atoi(owner.text)
		if err != nil {
			return fmt.Errorf("%w: service %s: %s.%s is not a number", ErrModel, s.Service, name, owner.key)
		}

		*owner.into = id
	}

	if use.Mode != nil {
		copyIn.Mode = fs.FileMode(*use.Mode)
	}

	s.CopyIn = append(s.CopyIn, copyIn)

	return nil
}
