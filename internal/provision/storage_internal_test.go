package provision

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Wintersta7e/stutter/internal/compose"
)

// Paths and flags the tests below name more than once.
const (
	pgVolume      = "/var/lib/postgresql"
	dataDir       = "/data"
	execForm      = "CMD"
	healthCmdFlag = "--health-cmd"
	echoArg       = "echo"
	noHealthFlag  = "--no-healthcheck"
	logsVolume    = "/logs"
	noneTest      = "NONE"
)

// execTest is an exec-form healthcheck test: CMD, then the argv.
func execTest(argv ...string) compose.Healthcheck {
	return compose.Healthcheck{Test: append([]string{execForm}, argv...)}
}

// hostDirs is a host where every path is a directory except those in files.
func hostDirs(files ...string) func(string) (bool, error) {
	return func(path string) (bool, error) { return !slices.Contains(files, path), nil }
}

// fresh, bind and tmpfs are compose mounts as compose.Spec carries them: every bind already read-only.
func fresh(target, volume string) compose.Mount {
	return compose.Mount{Kind: compose.MountFresh, Target: target, Volume: volume}
}

func bind(source, target string) compose.Mount {
	return compose.Mount{Kind: compose.MountBind, Source: source, Target: target, ReadOnly: true}
}

func tmpfs(target string) compose.Mount {
	return compose.Mount{Kind: compose.MountTmpfs, Target: target}
}

// writableAt marks the compose volumes entry at index as a bind compose declared writable, the way the
// model records it.
func writableAt(index string) compose.Replaced {
	return compose.Replaced{Key: "volumes[" + index + "]", What: "mounted read-only"}
}

// plannedTargets are the paths a plan puts on tmpfs, on binds, and on template volumes.
func plannedTargets(plan storagePlan) (tmpfsAt, bindAt, templateAt []string) { //nolint:nonamedreturns // three lists.
	for _, m := range plan.mounts {
		switch m.Kind {
		case compose.MountTmpfs:
			tmpfsAt = append(tmpfsAt, m.Target)
		case compose.MountBind:
			if !m.ReadOnly {
				bindAt = append(bindAt, "WRITABLE "+m.Target)
			}

			bindAt = append(bindAt, m.Target)
		case compose.MountFresh:
			templateAt = append(templateAt, "UNPLANNED "+m.Target)
		default:
			templateAt = append(templateAt, "UNKNOWN "+m.Target)
		}
	}

	for _, tmpl := range plan.templates {
		templateAt = append(templateAt, tmpl.target)
	}

	return tmpfsAt, bindAt, templateAt
}

// TestSeedStorageFollowsItsPath puts every writable path of a seed where its path says: on the Postgres
// path the image's VOLUME is a tmpfs and anything under it is dropped, on the other path every VOLUME
// and volume is a template; a user directory is copied, never written; each decision is named.
func TestSeedStorageFollowsItsPath(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name         string
		path         string
		tmpfsAt      []string
		bindAt       []string
		templateAt   []string
		verdicts     []string
		in           storageInput
		pgdata, fail bool
	}{
		{
			name: "postgres", path: PathPostgres,
			in: storageInput{imageVolumes: []string{pgVolume}, isDir: hostDirs(), spec: compose.Spec{
				Env: map[string]string{"PGDATA": "/var/lib/pgdata"},
				Mounts: []compose.Mount{
					fresh(pgVolume, "dbdata"), fresh("/var/run/postgresql", "dbsock"),
					bind("/src/init", "/docker-entrypoint-initdb.d"), bind("/src/conf", "/etc/conf"),
					fresh("/var/lib/pgdata/sub", ""), tmpfs("/scratch"),
				},
				Replaced: []compose.Replaced{writableAt("3")},
			}},
			tmpfsAt:    []string{pgVolume, "/scratch"},
			bindAt:     []string{"/docker-entrypoint-initdb.d", "/etc/conf"},
			templateAt: []string{"/var/run/postgresql"},
			verdicts: []string{
				"PGDATA replaced", "/var/lib/postgresql tmpfs", "/var/lib/postgresql dropped",
				"/var/run/postgresql template", "/etc/conf read-only", "/var/lib/pgdata/sub dropped",
			},
			pgdata: true,
		},
		{
			name: "other", path: PathOther,
			in: storageInput{
				imageVolumes: []string{dataDir, logsVolume}, isDir: hostDirs("/src/app.conf"),
				spec: compose.Spec{
					Mounts: []compose.Mount{
						fresh(dataDir, "cachedata"), bind("/src/work", "/work"),
						bind("/src/app.conf", "/conf/app.conf"), bind("/src/ro", "/ro"),
						tmpfs("/run"), fresh("/anon", ""),
					},
					Replaced: []compose.Replaced{writableAt("1"), writableAt("2")},
				},
			},
			tmpfsAt:    []string{"/run"},
			bindAt:     []string{"/conf/app.conf", "/ro"},
			templateAt: []string{logsVolume, dataDir, "/work", "/anon"},
			verdicts: []string{
				logsVolume + " template", "/data template", "/work copied", "/conf/app.conf read-only",
				"/anon template",
			},
		},
		{
			name: "a writable-bind marker on a volume", path: PathOther, fail: true,
			in: storageInput{isDir: hostDirs(), spec: compose.Spec{
				Mounts: []compose.Mount{fresh(dataDir, "cachedata")}, Replaced: []compose.Replaced{writableAt("0")},
			}},
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			plan, err := seedPlan(row.in, row.path)
			if row.fail {
				if !errors.Is(err, errStorage) {
					t.Fatalf("seedPlan() = %v, want errStorage", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("seedPlan() error = %v", err)
			}

			tmpfsAt, bindAt, templateAt := plannedTargets(plan)

			for _, check := range []struct {
				name      string
				got, want []string
			}{
				{name: "tmpfs", got: tmpfsAt, want: row.tmpfsAt},
				{name: "binds", got: bindAt, want: row.bindAt},
				{name: "templates", got: templateAt, want: row.templateAt},
				{name: "verdicts", got: plan.verdicts, want: row.verdicts},
			} {
				if !slices.Equal(check.got, check.want) {
					t.Errorf("%s = %q, want %q", check.name, check.got, check.want)
				}
			}

			if plan.pgdata != row.pgdata {
				t.Errorf("pgdata = %v, want %v", plan.pgdata, row.pgdata)
			}
		})
	}
}

// TestClassificationCoversEveryWritablePathWithTmpfs keeps a classification container off every volume:
// it runs once, only to be asked a question, and must leave nothing behind.
func TestClassificationCoversEveryWritablePathWithTmpfs(t *testing.T) {
	t.Parallel()

	plan, err := classificationPlan(storageInput{
		imageVolumes: []string{dataDir, logsVolume}, isDir: hostDirs("/src/app.conf"),
		spec: compose.Spec{
			Mounts: []compose.Mount{
				fresh(dataDir, "cachedata"), bind("/src/work", "/work"), bind("/src/app.conf", "/conf/app.conf"),
				bind("/src/ro", "/ro"), fresh("/anon", ""),
			},
			Replaced: []compose.Replaced{writableAt("1"), writableAt("2")},
		},
	})
	if err != nil {
		t.Fatalf("classificationPlan() error = %v", err)
	}

	tmpfsAt, bindAt, templateAt := plannedTargets(plan)

	if want := []string{logsVolume, dataDir, "/work", "/anon"}; !slices.Equal(tmpfsAt, want) {
		t.Errorf("tmpfs = %q, want %q", tmpfsAt, want)
	}

	if want := []string{"/conf/app.conf", "/ro"}; !slices.Equal(bindAt, want) {
		t.Errorf("binds = %q, want %q", bindAt, want)
	}

	if len(templateAt) != 0 {
		t.Errorf("templates = %q, want none: a classification container creates no volume", templateAt)
	}
}

// TestAJobSharesTheDependencysTemplate mounts a volume a job shares with a started dependency as that
// dependency's template, so what the job writes is what the snapshot holds.
func TestAJobSharesTheDependencysTemplate(t *testing.T) {
	t.Parallel()

	shared := &Volume{name: "template-shared"}

	mounts, volumes := jobVolumes(compose.Spec{Mounts: []compose.Mount{
		fresh(dataDir, "shared"), fresh("/scratch", "own"), bind("/src/sql", "/sql"),
	}}, map[string]*Volume{"shared": shared})

	if len(volumes) != 1 || volumes[0].Volume != shared || volumes[0].Target != dataDir {
		t.Fatalf("volumes = %+v, want the dependency's template at /data", volumes)
	}

	if !slices.EqualFunc(mounts, []compose.Mount{fresh("/scratch", "own"), bind("/src/sql", "/sql")},
		func(a, b compose.Mount) bool { return a == b }) {
		t.Errorf("mounts = %+v, want the job's own volume and bind untouched", mounts)
	}
}

// TestTwoDependenciesNeverShareATemplate gives each of two started dependencies its own copy of a
// volume they share, and says so in both records: a channel between them would outlive a restore.
func TestTwoDependenciesNeverShareATemplate(t *testing.T) {
	t.Parallel()

	in := storageInput{
		isDir: hostDirs(), shared: map[string]bool{"common": true},
		spec: compose.Spec{Mounts: []compose.Mount{fresh("/common", "common"), fresh("/mine", "mine")}},
	}

	plan, err := seedPlan(in, PathOther)
	if err != nil {
		t.Fatalf("seedPlan() error = %v", err)
	}

	if want := []string{"/common private", "/mine template"}; !slices.Equal(plan.verdicts, want) {
		t.Errorf("verdicts = %q, want %q", plan.verdicts, want)
	}

	if len(plan.templates) != 2 {
		t.Errorf("templates = %+v, want one each for /common and /mine", plan.templates)
	}
}

// TestAnExecHealthcheckBecomesOneShellString passes a compose healthcheck to the engine, which takes
// only a shell command: an exec-form test must reach it as a shell string that splits back into exactly
// its arguments, whatever they hold.
func TestAnExecHealthcheckBecomesOneShellString(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name  string
		want  []string
		check compose.Healthcheck
		set   bool
	}{
		{
			name: "plain words", set: true, check: execTest("pg_isready", "-q"),
			want: []string{healthCmdFlag, `'pg_isready' '-q'`},
		},
		{
			name: "spaces", set: true, check: execTest("test", "-f", "/tmp/a b"),
			want: []string{healthCmdFlag, `'test' '-f' '/tmp/a b'`},
		},
		{
			name: "a single quote", set: true, check: execTest(echoArg, "it's"),
			want: []string{healthCmdFlag, `'echo' 'it'\''s'`},
		},
		{
			name: "double quotes", set: true, check: execTest(echoArg, `say "hi"`),
			want: []string{healthCmdFlag, `'echo' 'say "hi"'`},
		},
		{
			name: "dollar and backquote", set: true, check: execTest(echoArg, "$HOME`x`"),
			want: []string{healthCmdFlag, "'echo' '$HOME`x`'"},
		},
		{
			name: "a backslash", set: true, check: execTest(echoArg, `a\b`),
			want: []string{healthCmdFlag, `'echo' 'a\b'`},
		},
		{
			name: "an empty argument", set: true, check: execTest(echoArg, ""),
			want: []string{healthCmdFlag, `'echo' ''`},
		},
		{
			name: "a newline", set: true, check: execTest(echoArg, "a\nb"),
			want: []string{healthCmdFlag, "'echo' 'a\nb'"},
		},
		{
			name: "shell form", set: true, check: compose.Healthcheck{Test: []string{"CMD-SHELL", "pg_isready -q"}},
			want: []string{healthCmdFlag, "pg_isready -q"},
		},
		{
			name: "NONE", set: true, check: compose.Healthcheck{Test: []string{noneTest}, Disabled: true},
			want: []string{noHealthFlag},
		},
		{name: "disable", set: true, check: compose.Healthcheck{Disabled: true}, want: []string{noHealthFlag}},
		{name: "absent", set: false, want: nil},
		{name: "timings", set: true, check: compose.Healthcheck{
			Test: []string{execForm, "true"}, Interval: time.Second, Timeout: 2 * time.Second, Retries: 3,
		}, want: []string{
			healthCmdFlag, "'true'", "--health-interval", "1s", "--health-timeout", "2s",
			"--health-retries", "3",
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			var got []string
			for _, flag := range healthArgs(healthcheckOf(row.check, row.set)) {
				got = append(got, flag.val)
			}

			if !slices.Equal(got, row.want) {
				t.Errorf("healthcheck flags = %q, want %q", got, row.want)
			}
		})
	}
}
