package scalegen

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
)

// ruleOrder mirrors the VS http[] order (first-match wins in Envoy).
var ruleOrder = []string{"exact", "prefix", "regex", "default"}

// samplePath returns a request path that hits the given rule of VS (i,j).
func samplePath(rule string, j int) string {
	switch rule {
	case "exact":
		return "/healthz"
	case "prefix":
		return fmt.Sprintf("/api/v1/users/%d", j)
	case "regex":
		return fmt.Sprintf("/products/%d", j+1)
	default: // catch-all
		return fmt.Sprintf("/misc/%d", j)
	}
}

// Cases builds the by-construction query corpus: >=1 hit per VS (rotating rule
// so all match kinds are exercised, and every hit host also matches the broad
// *.example.com so gwresolve must pick most-specific), plus catch-all cases,
// broad-gateway empty-RC misses, and unknown-host misses. Deterministic by seed.
func (g *Gen) Cases(seed int64) []Case {
	rng := rand.New(rand.NewSource(seed))
	cases := make([]Case, 0, g.cfg.NumGateways*g.cfg.VSPerGW+g.cfg.NumGateways)

	for i := 0; i < g.cfg.NumGateways; i++ {
		for j := 0; j < g.cfg.VSPerGW; j++ {
			rule := ruleOrder[(i+j)%len(ruleOrder)]
			cases = append(cases, Case{
				Gateway:  gwName(i),
				Host:     vsHost(i, j),
				Path:     samplePath(rule, j),
				Expected: clusterOf(i, j, rule),
			})
			// ~5% also get a weird path -> falls to catch-all (default cluster).
			if rng.Intn(20) == 0 {
				cases = append(cases, Case{
					Gateway:  gwName(i),
					Host:     vsHost(i, j),
					Path:     fmt.Sprintf("/zz-nomatch-%d", j),
					Expected: clusterOf(i, j, "default"),
				})
			}
		}
	}

	// Broad-gateway hosts: match only *.example.com -> resolve to broad, whose
	// scoped RC has no VS -> 404 miss (Expected "").
	nMiss := g.cfg.NumGateways / 4
	for k := 0; k < nMiss; k++ {
		cases = append(cases, Case{
			Gateway:  broadName,
			Host:     fmt.Sprintf("direct-%d.example.com", k),
			Path:     "/anything",
			Expected: "",
		})
		// Unknown hosts: match no gateway at all.
		cases = append(cases, Case{
			Gateway:  "",
			Host:     fmt.Sprintf("nope-%d.unknown.invalid", k),
			Path:     "/x",
			Expected: "",
		})
	}

	rng.Shuffle(len(cases), func(a, b int) { cases[a], cases[b] = cases[b], cases[a] })
	return cases
}

// WriteYAML writes valid, applyable Istio YAML for the first sampleGateways
// gateways (gateways.yaml + virtualservices.yaml + services.yaml) for audit and
// the validity spot-check. Not used to feed istiod (that path is typed configs).
func (g *Gen) WriteYAML(dir string, sampleGateways int) error {
	if sampleGateways > g.cfg.NumGateways {
		sampleGateways = g.cfg.NumGateways
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var gws, vss, svcs strings.Builder
	for i := 0; i < sampleGateways; i++ {
		writeGatewayYAML(&gws, i)
		for j := 0; j < g.cfg.VSPerGW; j++ {
			writeVSYAML(&vss, i, j)
			for _, rule := range ruleOrder {
				writeServiceYAML(&svcs, i, j, rule)
			}
		}
	}
	for name, b := range map[string]string{
		"gateways.yaml":        gws.String(),
		"virtualservices.yaml": vss.String(),
		"services.yaml":        svcs.String(),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(b), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func writeGatewayYAML(b *strings.Builder, i int) {
	fmt.Fprintf(b, `---
apiVersion: networking.istio.io/v1beta1
kind: Gateway
metadata:
  name: %s
  namespace: %s
spec:
  selector:
    istio: %s
  servers:
    - port:
        number: 80
        name: http
        protocol: HTTP
      hosts:
        - "%s"
    - port:
        number: 443
        name: https
        protocol: HTTPS
      tls:
        mode: SIMPLE
        credentialName: cred-%s
      hosts:
        - "%s"
`, gwName(i), gwNS, gwName(i), gwHostPat(i), pad3(i), gwHostPat(i))
}

func writeVSYAML(b *strings.Builder, i, j int) {
	fmt.Fprintf(b, `---
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: vs-%s-%s
  namespace: %s
spec:
  hosts:
    - %s
  gateways:
    - %s/%s
  http:
    - match:
        - uri:
            exact: /healthz
      route:
        - destination:
            host: %s
            port:
              number: %d
    - match:
        - uri:
            prefix: /api/v1
      route:
        - destination:
            host: %s
            port:
              number: %d
    - match:
        - uri:
            regex: %s
      route:
        - destination:
            host: %s
            port:
              number: %d
    - route:
        - destination:
            host: %s
            port:
              number: %d
`, pad3(i), pad2(j), vsNS(i), vsHost(i, j), gwNS, gwName(i),
		destFQDN(i, j, "exact"), svcPort,
		destFQDN(i, j, "prefix"), svcPort,
		regexRule, destFQDN(i, j, "regex"), svcPort,
		destFQDN(i, j, "default"), svcPort)
}

func writeServiceYAML(b *strings.Builder, i, j int, rule string) {
	fmt.Fprintf(b, `---
apiVersion: v1
kind: Service
metadata:
  name: %s
  namespace: %s
spec:
  ports:
`, destShort(i, j, rule), vsNS(i))
	for _, p := range backendPorts(i, j) {
		fmt.Fprintf(b, "    - name: %s\n      port: %d\n      protocol: %s\n", p.Name, p.Port, p.Protocol)
	}
}
