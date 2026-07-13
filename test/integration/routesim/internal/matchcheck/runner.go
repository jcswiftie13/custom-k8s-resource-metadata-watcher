// Package matchcheck drives Envoy's router_check_tool (offline, no traffic) over
// istiod-translated RouteConfigurations to resolve each host+path to its cluster.
// It is the sole route-resolution engine.
//
// NATIVE ONLY: unlike the PoC original, there is no docker-run fallback. The
// tool ships prebuilt in the Envoy "tools" image (envoyproxy/envoy:tools-<ver>);
// the e2e harness runs this whole test binary INSIDE that image, where
// router_check_tool is a native binary at /usr/local/bin/router_check_tool.
// A missing tool is an environment error the caller must treat as fatal — never
// a skip.
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

// routerCheckBin is the tool's binary name for the PATH fallback.
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

// Runner executes Envoy's router_check_tool (native binary) over a
// RouteConfiguration + a batch of test cases.
type Runner struct {
	bin string
}

// New returns a Runner for the native router_check_tool at bin, or — when bin is
// empty — the first "router_check_tool" on PATH. It fails when the tool is
// missing or not runnable; callers must treat that as fatal (the resolution
// engine cannot run without it), never as a skip.
func New(bin string) (Runner, error) {
	if bin == "" {
		p, err := exec.LookPath(routerCheckBin)
		if err != nil {
			return Runner{}, fmt.Errorf("router_check_tool not found on PATH and no explicit path given: %w", err)
		}
		bin = p
	}
	if out, err := exec.Command(bin, "--version").CombinedOutput(); err != nil {
		return Runner{}, fmt.Errorf("router_check_tool at %s is not runnable: %v\n%s", bin, err, out)
	}
	return Runner{bin: bin}, nil
}

// exec runs the tool over cfgName + testsName (both relative to workDir) and
// returns combined stdout+stderr. --disable-deprecation-check is always set:
// istiod-translated RCs carry deprecated fields (e.g. RouteAction.max_grpc_timeout)
// that newer Envoy otherwise rejects at load.
func (r Runner) exec(ctx context.Context, workDir, cfgName, testsName string) ([]byte, error) {
	return exec.CommandContext(ctx, r.bin,
		"-c", filepath.Join(workDir, cfgName),
		"-t", filepath.Join(workDir, testsName),
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
		return nil, fmt.Errorf("router_check_tool resolve: %v: %v\n%s", perr, runErr, raw)
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
