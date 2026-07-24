package matchcheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"google.golang.org/protobuf/encoding/protojson"
)

// ScaleCase is one offline cross-validation case (host+path -> expected cluster).
type ScaleCase struct {
	Gateway  string
	Host     string
	Path     string
	Expected string // by-construction oracle; only hit cases (non-empty) are checked
}

// RunScale offline-validates each hit case against its Expected cluster using
// the prebuilt router_check_tool (matcher A), one invocation per gateway with
// bounded concurrency. rcFor returns the RAW (un-echoed, real cluster names)
// RouteConfiguration for a gateway. Returns per-gateway agreement counts.
func RunScale(ctx context.Context, image string, rcFor func(gw string) *route.RouteConfiguration, cases []ScaleCase, maxConc int) (agree, disagree int, elapsed time.Duration, err error) {
	runner := runnerFor(image)
	if maxConc <= 0 {
		maxConc = 8
	}

	// group hit cases per gateway
	perGW := map[string][]ScaleCase{}
	for _, c := range cases {
		if c.Expected == "" || c.Gateway == "" {
			continue // misses aren't cross-checked here (404 semantics vary)
		}
		perGW[c.Gateway] = append(perGW[c.Gateway], c)
	}

	work, err := os.MkdirTemp("", "rc-scale-*")
	if err != nil {
		return 0, 0, 0, err
	}
	defer os.RemoveAll(work)

	type res struct {
		agree, disagree int
		dur             time.Duration
		err             error
	}
	sem := make(chan struct{}, maxConc)
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := []res{}

	for gw, gcases := range perGW {
		rc := rcFor(gw)
		if rc == nil {
			return 0, 0, 0, fmt.Errorf("no RC for gateway %q", gw)
		}
		rcJSON, e := protojson.Marshal(rc)
		if e != nil {
			return 0, 0, 0, e
		}
		rcPath := filepath.Join(work, gw+"-rc.json")
		if e := os.WriteFile(rcPath, rcJSON, 0o644); e != nil {
			return 0, 0, 0, e
		}
		tests := make([]routerCheckTest, 0, len(gcases))
		for i, c := range gcases {
			tests = append(tests, routerCheckTest{
				TestName: fmt.Sprintf("%s-%d", gw, i),
				Input:    rcInput{Authority: c.Host, Path: c.Path, Method: "GET"},
				Validate: rcValid{ClusterName: c.Expected},
			})
		}
		testsJSON, _ := json.Marshal(map[string]any{"tests": tests})
		testsPath := filepath.Join(work, gw+"-tests.json")
		if e := os.WriteFile(testsPath, testsJSON, 0o644); e != nil {
			return 0, 0, 0, e
		}

		wg.Add(1)
		go func(gw string, n int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			start := time.Now()
			out, runErr := runner.exec(ctx, work, gw+"-rc.json", gw+"-tests.json")
			dur := time.Since(start)
			fails := bytes.Count(out, []byte("Failure"))
			r := res{agree: n - fails, disagree: fails, dur: dur}
			if runErr != nil && fails == 0 {
				r.err = fmt.Errorf("router_check_tool (%s): %v\n%s", gw, runErr, out)
			}
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}(gw, len(gcases))
	}
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return 0, 0, 0, r.err
		}
		agree += r.agree
		disagree += r.disagree
		if r.dur > elapsed {
			elapsed = r.dur // wall-ish: slowest invocation (they run concurrently)
		}
	}
	return agree, disagree, elapsed, nil
}
