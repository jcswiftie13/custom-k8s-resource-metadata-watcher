// Package ingload generates the ingress-traffic corpus from scalegen and streams
// it into ClickHouse as multi-version rows. It is the single loader shared by
// cmd/ipflow (-mode=load) and the end-to-end test, so the "program-generated,
// never hand-written SQL" test data is produced in exactly one place.
package ingload

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"istio.io/istio/pkg/config"

	"github.com/example/metadata-exporter/poc/route2a/internal/chstore"
	"github.com/example/metadata-exporter/poc/route2a/internal/scalegen"
)

// Versions holds the per-resource-type version counts (bitemporal depth).
type Versions struct {
	Deploy, Svc, Gw, VS, KSvc int
}

// DefaultVersions matches the design spec: Deployment/Service/Gateway ×2,
// VirtualService ×10, backend k8s Service ×100.
func DefaultVersions() Versions { return Versions{Deploy: 2, Svc: 2, Gw: 2, VS: 10, KSvc: 100} }

// Progress is an optional per-gateway progress callback (nil = silent).
type Progress func(done, total int)

// Load (re)creates the schema and streams the whole corpus into ClickHouse.
func Load(ctx context.Context, store *chstore.Store, gen *scalegen.Gen, v Versions, prog Progress) error {
	if err := store.CreateSchema(ctx); err != nil {
		return err
	}

	var seq uint64
	next := func() uint64 { seq++; return seq }
	empty := []string{}

	svcB, err := store.NewServiceBatch(ctx)
	if err != nil {
		return err
	}
	depB, err := store.NewDeployBatch(ctx)
	if err != nil {
		return err
	}
	gwB, err := store.NewGwBatch(ctx)
	if err != nil {
		return err
	}
	vsB, err := store.NewVSBatch(ctx)
	if err != nil {
		return err
	}

	n := gen.NumGateways()
	for i := 0; i < n; i++ {
		// ingress LB Service.
		svc := gen.IngressService(i)
		sel := scalegen.SortedKV(svc.Selector)
		for _, ver := range chstore.Versions(v.Svc) {
			if err := svcB.Append(chstore.ServiceRow{
				Namespace: svc.Namespace, Name: svc.Name, ValidFrom: ver.From, ValidTo: ver.To, Rev: uint32(ver.Rev),
				IngressIPs: []string{svc.IP}, Selector: sel, IngestSeq: next(),
			}); err != nil {
				return err
			}
		}
		// backend destination Services.
		for _, bs := range gen.BackendServices(i) {
			for _, ver := range chstore.Versions(v.KSvc) {
				if err := svcB.Append(chstore.ServiceRow{
					Namespace: bs.Namespace, Name: bs.Name, ValidFrom: ver.From, ValidTo: ver.To, Rev: uint32(ver.Rev),
					IngressIPs: empty, Selector: empty, Hostname: bs.Hostname, Port: uint32(bs.Port),
					PortName: bs.PortName, Protocol: bs.Protocol, IngestSeq: next(),
				}); err != nil {
					return err
				}
			}
		}
		// ingress workload Deployment.
		dep := gen.IngressDeployment(i)
		pod := scalegen.SortedKV(dep.PodLabels)
		for _, ver := range chstore.Versions(v.Deploy) {
			if err := depB.Append(dep.Namespace, dep.Name, ver.From, ver.To, uint32(ver.Rev), pod, next()); err != nil {
				return err
			}
		}
		// Gateway CR.
		gwJSON, err := marshalSpec(gen.GatewayConfig(i))
		if err != nil {
			return err
		}
		gwSel := scalegen.SortedKV(gen.GatewaySelector(i))
		hosts := []string{scalegen.GatewayHostPattern(i)}
		for _, ver := range chstore.Versions(v.Gw) {
			if err := gwB.Append(scalegen.GatewayNamespace(), scalegen.GatewayName(i), ver.From, ver.To, uint32(ver.Rev), gwSel, hosts, gwJSON, next()); err != nil {
				return err
			}
		}
		// VirtualServices.
		bound := []string{scalegen.BoundGatewayRef(i)}
		for j := 0; j < gen.VSPerGW(); j++ {
			vsCfg := gen.VSConfig(i, j)
			vsJSON, err := marshalSpec(vsCfg)
			if err != nil {
				return err
			}
			for _, ver := range chstore.Versions(v.VS) {
				if err := vsB.Append(vsCfg.Meta.Namespace, vsCfg.Meta.Name, ver.From, ver.To, uint32(ver.Rev), bound, vsJSON, next()); err != nil {
					return err
				}
			}
		}
		if prog != nil {
			prog(i+1, n)
		}
	}
	for _, c := range []func() error{svcB.Close, depB.Close, gwB.Close, vsB.Close} {
		if err := c(); err != nil {
			return err
		}
	}
	return nil
}

// marshalSpec protojson-marshals a config.Config's proto Spec into spec_json.
func marshalSpec(cfg config.Config) (string, error) {
	m, ok := cfg.Spec.(proto.Message)
	if !ok {
		return "", fmt.Errorf("spec %T is not a proto.Message", cfg.Spec)
	}
	b, err := protojson.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
