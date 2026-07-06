// Package matchcheck drives Envoy's router_check_tool (offline, no traffic) over
// istiod-translated RouteConfigurations to resolve each host+path to its cluster.
// It is the sole route-resolution engine (the live-Envoy path was removed).
//
// One Runner (native binary preferred, docker fallback) backs two entry points:
//   - Runner.Resolve  — production "host+path -> service": returns the matched
//     cluster per query, no expected answer needed (see below).
//   - RunScale        — validator/agreement mode: cross-check a batch against a
//     known-expected cluster (used by the oracle correctness test, see scale.go).
//
// router_check_tool takes one RouteConfiguration + a batch of test cases, so we
// invoke it once per gateway (filtering cases to that gateway).
//
// NOTE: router_check_tool ships prebuilt in the Envoy "tools" image
// (envoyproxy/envoy:tools-<ver>) — no bazel build needed. Exact CLI flags can
// vary by Envoy version.
package matchcheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"google.golang.org/protobuf/encoding/protojson"
)

// DefaultImage is the prebuilt Envoy tools image (ships router_check_tool).
// Overridable via POC_ROUTERCHECK_IMAGE.
const DefaultImage = "envoyproxy/envoy:tools-v1.34-latest"

// routerCheckBin is the tool's path/name (native binary, or the docker --entrypoint).
const routerCheckBin = "router_check_tool"

// routerCheckTest is one entry of the router_check_tool test file.
type routerCheckTest struct {
	TestName string  `json:"test_name"`
	Input    rcInput `json:"input"`
	Validate rcValid `json:"validate"`
}

type rcInput struct {
	Authority string `json:"authority"`
	Path      string `json:"path"`
	Method    string `json:"method"`
}

type rcValid struct {
	ClusterName string `json:"cluster_name"`
}

// Runner executes Envoy's router_check_tool over a RouteConfiguration + a batch
// of test cases. It runs the tool either as a NATIVE binary (preferred: no
// per-batch container-startup tax, so timings reflect real resolution cost) or,
// when no binary is available, via `docker run` on the prebuilt tools image.
type Runner struct {
	bin   string // native router_check_tool path; "" => use docker
	image string // docker image, used only when bin == ""
}

// nativeBin returns a usable native router_check_tool path, or "".
func nativeBin() string {
	if b := os.Getenv("POC_ROUTERCHECK_BIN"); b != "" {
		return b
	}
	if p, err := exec.LookPath(routerCheckBin); err == nil {
		return p
	}
	return ""
}

// dockerImage returns the tools image (POC_ROUTERCHECK_IMAGE or DefaultImage).
func dockerImage() string {
	if img := os.Getenv("POC_ROUTERCHECK_IMAGE"); img != "" {
		return img
	}
	return DefaultImage
}

// Detect picks the execution mode: a native router_check_tool binary if present
// (POC_ROUTERCHECK_BIN or on PATH), else docker with the tools image if the
// daemon is up and the image is pulled. kind is "native" or "docker"; ok is
// false when neither is available (callers should skip).
func Detect() (r Runner, kind string, ok bool) {
	if b := nativeBin(); b != "" {
		return Runner{bin: b}, "native", true
	}
	img := dockerImage()
	if err := exec.Command("docker", "info").Run(); err != nil {
		return Runner{}, "", false
	}
	if err := exec.Command("docker", "image", "inspect", img).Run(); err != nil {
		return Runner{}, "", false
	}
	return Runner{image: img}, "docker", true
}

// runnerFor returns a native runner when a router_check_tool binary is available,
// otherwise a docker runner bound to image (or the default tools image if empty).
func runnerFor(image string) Runner {
	if b := nativeBin(); b != "" {
		return Runner{bin: b}
	}
	if image == "" {
		image = dockerImage()
	}
	return Runner{image: image}
}

// Kind reports how this runner executes ("native" or "docker").
func (r Runner) Kind() string {
	if r.bin != "" {
		return "native"
	}
	return "docker"
}

// exec runs the tool over cfgName + testsName (both relative to workDir) and
// returns combined stdout+stderr. --disable-deprecation-check is always set:
// istiod-translated RCs carry deprecated fields (e.g. RouteAction.max_grpc_timeout)
// that newer Envoy otherwise rejects at load.
func (r Runner) exec(ctx context.Context, workDir, cfgName, testsName string) ([]byte, error) {
	if r.bin != "" {
		return exec.CommandContext(ctx, r.bin,
			"-c", filepath.Join(workDir, cfgName),
			"-t", filepath.Join(workDir, testsName),
			"--details", "--disable-deprecation-check",
		).CombinedOutput()
	}
	return exec.CommandContext(ctx, "docker", "run", "--rm",
		"--entrypoint", routerCheckBin,
		"-v", workDir+":/work:ro", r.image,
		"-c", "/work/"+cfgName,
		"-t", "/work/"+testsName,
		"--details", "--disable-deprecation-check",
	).CombinedOutput()
}

// Query is one host+path to resolve (method is fixed to GET).
type Query struct {
	Host string
	Path string
}

// resolveSentinel is an expected cluster value no real route can equal, so
// router_check_tool reports every case as a (forced) mismatch and prints the
// real matched cluster in its "actual: [...]" detail — turning the validator
// into a resolver.
const resolveSentinel = "__routecheck_unmatched_sentinel__"

// Resolve returns, per query, the destination cluster that rc routes it to
// ("" = no route / miss), using router_check_tool as the matching engine. This
// is the production "host+path -> service" primitive: it needs no expected
// answer. One tool invocation covers the whole batch.
func (r Runner) Resolve(ctx context.Context, rc *route.RouteConfiguration, queries []Query) ([]string, error) {
	out := make([]string, len(queries))
	if len(queries) == 0 {
		return out, nil
	}

	work, err := os.MkdirTemp("", "rc-resolve-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(work)

	rcJSON, err := protojson.Marshal(rc)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(work, "rc.json"), rcJSON, 0o644); err != nil {
		return nil, err
	}

	tests := make([]routerCheckTest, len(queries))
	for i, q := range queries {
		tests[i] = routerCheckTest{
			TestName: strconv.Itoa(i),
			Input:    rcInput{Authority: q.Host, Path: q.Path, Method: "GET"},
			Validate: rcValid{ClusterName: resolveSentinel},
		}
	}
	testsJSON, _ := json.Marshal(map[string]any{"tests": tests})
	if err := os.WriteFile(filepath.Join(work, "tests.json"), testsJSON, 0o644); err != nil {
		return nil, err
	}

	// The tool exits non-zero because every case "fails" the sentinel — expected.
	// A genuine failure (config won't load) yields no per-case detail lines, which
	// parseActuals reports as an error.
	raw, runErr := r.exec(ctx, work, "rc.json", "tests.json")
	clusters, perr := parseActuals(raw, len(queries))
	if perr != nil {
		return nil, fmt.Errorf("router_check_tool resolve (%s): %v: %v\n%s", r.Kind(), perr, runErr, raw)
	}
	copy(out, clusters)
	return out, nil
}

var actualMarker = []byte("actual: [")

// parseActuals reads router_check_tool --details output. Each case prints its
// test_name (our decimal index) on its own line, followed by a line containing
// "actual: [<cluster>]" (empty brackets == miss). Returns one cluster per index;
// errors only if NOT A SINGLE case produced a detail line (i.e. the tool failed
// before running tests).
func parseActuals(out []byte, n int) ([]string, error) {
	res := make([]string, n)
	filled := make([]bool, n)
	got := 0
	cur := -1
	for _, line := range bytes.Split(out, []byte("\n")) {
		s := bytes.TrimSpace(line)
		if idx, err := strconv.Atoi(string(s)); err == nil {
			if idx >= 0 && idx < n {
				cur = idx
			} else {
				cur = -1
			}
			continue
		}
		if i := bytes.Index(s, actualMarker); i >= 0 && cur >= 0 {
			rest := s[i+len(actualMarker):]
			if j := bytes.IndexByte(rest, ']'); j >= 0 {
				if !filled[cur] {
					res[cur] = string(rest[:j])
					filled[cur] = true
					got++
				}
			}
			cur = -1
		}
	}
	if got == 0 {
		return nil, fmt.Errorf("no per-case results parsed")
	}
	return res, nil
}
