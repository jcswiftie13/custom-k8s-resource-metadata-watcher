// This file productionizes the scoped translation path. Where scoped.go's
// RoutesForScoped leans on istio's TEST fixture core.NewConfigGenTest (which
// ties a stop channel to t.Cleanup and starts two long-lived goroutines that
// only exit when the whole test ends — a goroutine/memory leak when called in a
// loop), Translator builds the minimal, STATIC config-generator world by hand:
// no controllers running, no goroutines, no dependency on istio's pkg/test.
//
// Key fact that makes this safe: PushContext.InitContext reads config via
// env.List(gvk.VirtualService, ...) directly from the store (push_context.go),
// not from an event-driven index — so the controllers' Run loops are not needed
// once the configs are Create()d into the store before InitContext.
package translate

import (
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"

	configaggregate "istio.io/istio/pilot/pkg/config/aggregate"
	"istio.io/istio/pilot/pkg/config/memory"
	"istio.io/istio/pilot/pkg/model"
	core "istio.io/istio/pilot/pkg/networking/core"
	"istio.io/istio/pilot/pkg/serviceregistry"
	"istio.io/istio/pilot/pkg/serviceregistry/aggregate"
	memregistry "istio.io/istio/pilot/pkg/serviceregistry/memory"
	"istio.io/istio/pilot/pkg/serviceregistry/provider"
	"istio.io/istio/pilot/pkg/serviceregistry/serviceentry"
	cluster2 "istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/config/mesh"
	"istio.io/istio/pkg/config/mesh/meshwatcher"
	"istio.io/istio/pkg/config/schema/collections"
)

// Translator is a process-lifetime, concurrency-safe translation core. The only
// state it holds is stateless/reusable (the config generator + default mesh);
// every Translate call builds its own throwaway world, so calls share nothing
// mutable and may run in parallel.
type Translator struct {
	configGen *core.ConfigGeneratorImpl
}

// NewTranslator builds the reusable config generator once. Cheap; safe to keep
// one per process (or one per goroutine — it carries no per-request state).
func NewTranslator() *Translator {
	return &Translator{
		configGen: core.NewConfigGenerator(&model.DisabledCache{}),
	}
}

// Translate runs istio's config-generator core over one ingress's scoped config
// and returns the http.80 RouteConfiguration istiod would push to it. Unlike
// RoutesForScoped it needs no test.Failer and starts no goroutines: the scoped
// world is assembled statically and discarded when this returns.
func (tr *Translator) Translate(in ScopedInput) (*route.RouteConfiguration, error) {
	env, err := buildScopedEnv(in)
	if err != nil {
		return nil, err
	}
	pc := env.PushContext()

	proxy := setupProxy(env, pc, in.Proxy)

	res, _ := tr.configGen.BuildHTTPRoutes(proxy, &model.PushRequest{Push: pc}, []string{RouteConfigName})
	for _, r := range res {
		rc := &route.RouteConfiguration{}
		if err := r.Resource.UnmarshalTo(rc); err != nil {
			return nil, err
		}
		if rc.GetName() == RouteConfigName {
			return rc, nil
		}
	}
	// No VS matched: an empty (all-miss) RC is the faithful result.
	return &route.RouteConfiguration{Name: RouteConfigName}, nil
}

// buildScopedEnv assembles the minimal static Environment for one gateway's
// scoped config, mirroring core.NewConfigGenTest but WITHOUT running any
// controller goroutine. Configs are Create()d synchronously before InitContext,
// which lists them straight from the store.
func buildScopedEnv(in ScopedInput) (*model.Environment, error) {
	cc := memory.NewSyncController(memory.MakeSkipValidation(collections.PilotGatewayAPI()))
	configController, _ := configaggregate.MakeWriteableCache([]model.ConfigStoreController{cc}, cc)

	m := mesh.DefaultMeshConfig()
	env := model.NewEnvironment()
	env.Watcher = meshwatcher.NewTestWatcher(m)

	xdsUpdater := model.NewEndpointIndexUpdater(env.EndpointIndex)

	serviceDiscovery := aggregate.NewController(aggregate.Options{})
	se := serviceentry.NewController(configController, xdsUpdater, env.Watcher)
	serviceDiscovery.AddRegistry(se)

	msd := memregistry.NewServiceDiscovery(in.Services...)
	msd.XdsUpdater = xdsUpdater
	msd.ClusterID = cluster2.ID(provider.Mock)
	serviceDiscovery.AddRegistry(serviceregistry.Simple{
		ClusterID:           cluster2.ID(provider.Mock),
		ProviderID:          provider.Mock,
		DiscoveryController: msd,
	})

	env.ServiceDiscovery = serviceDiscovery
	env.ConfigStore = configController
	env.NetworksWatcher = meshwatcher.NewFixedNetworksWatcher(nil)
	env.Init()

	// Populate the store BEFORE InitContext; InitContext lists directly from it.
	for _, cfg := range in.Configs {
		if _, err := configController.Create(cfg); err != nil {
			return nil, err
		}
	}

	if err := env.InitNetworksManager(xdsUpdater); err != nil {
		return nil, err
	}
	env.PushContext().InitContext(env, nil, nil)
	return env, nil
}

// setupProxy fills the same proxy defaults core.ConfigGenTest.SetupProxy does
// for a gateway vantage, then wires it to the freshly built PushContext.
func setupProxy(env *model.Environment, pc *model.PushContext, gw GatewayProxy) *model.Proxy {
	p := &model.Proxy{
		Type:            model.Router,
		ConfigNamespace: gw.Namespace,
		Labels:          gw.Labels,
		Metadata:        &model.NodeMetadata{Namespace: gw.Namespace, Labels: gw.Labels},
	}
	if p.Metadata.IstioVersion == "" {
		p.Metadata.IstioVersion = "1.23.0"
	}
	p.IstioVersion = model.ParseIstioVersion(p.Metadata.IstioVersion)
	if p.ConfigNamespace == "" {
		p.ConfigNamespace = "default"
	}
	if p.Metadata.Namespace == "" {
		p.Metadata.Namespace = p.ConfigNamespace
	}
	if p.ID == "" {
		p.ID = "app.test"
	}
	if p.DNSDomain == "" {
		p.DNSDomain = p.ConfigNamespace + ".svc.cluster.local"
	}
	if len(p.IPAddresses) == 0 {
		p.IPAddresses = []string{"1.1.1.1"}
	}

	p.SetSidecarScope(pc)
	p.SetServiceTargets(env.ServiceDiscovery)
	p.SetGatewaysForProxy(pc)
	p.DiscoverIPMode()
	return p
}
