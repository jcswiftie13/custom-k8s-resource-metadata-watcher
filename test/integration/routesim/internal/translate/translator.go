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
	"fmt"

	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	networking "istio.io/api/networking/v1alpha3"

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
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/gateway"
	"istio.io/istio/pkg/config/mesh"
	"istio.io/istio/pkg/config/mesh/meshwatcher"
	"istio.io/istio/pkg/config/protocol"
	"istio.io/istio/pkg/config/schema/collections"
	"istio.io/istio/pkg/config/schema/gvk"
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
// and returns the RouteConfiguration istiod would push to it for the listener
// port in.Port (default 80). Unlike RoutesForScoped it needs no test.Failer and
// starts no goroutines: the scoped world is assembled statically and discarded
// when this returns.
func (tr *Translator) Translate(in ScopedInput) (*route.RouteConfiguration, error) {
	port := in.Port
	if port <= 0 {
		port = 80
	}
	gwCfg, ok := gatewayConfig(in.Configs)
	if !ok {
		return nil, fmt.Errorf("translate: scoped input has no Gateway config")
	}
	name, ok := routeConfigNameFor(gwCfg, port)
	if !ok {
		// No HTTP RDS route for this listener port (no server on the port, or a
		// TLS-passthrough server): faithfully a miss (empty RC).
		return &route.RouteConfiguration{Name: fmt.Sprintf("http.%d", port)}, nil
	}

	env, err := buildScopedEnv(in)
	if err != nil {
		return nil, err
	}
	pc := env.PushContext()

	proxy := setupProxy(env, pc, in.Proxy)

	res, _ := tr.configGen.BuildHTTPRoutes(proxy, &model.PushRequest{Push: pc}, []string{name})
	for _, r := range res {
		rc := &route.RouteConfiguration{}
		if err := r.Resource.UnmarshalTo(rc); err != nil {
			return nil, err
		}
		if rc.GetName() == name {
			return rc, nil
		}
	}
	// No VS matched: an empty (all-miss) RC is the faithful result.
	return &route.RouteConfiguration{Name: name}, nil
}

// gatewayConfig returns the single Gateway CR from a scoped input's configs.
func gatewayConfig(configs []config.Config) (config.Config, bool) {
	for _, c := range configs {
		if c.GroupVersionKind == gvk.Gateway {
			return c, true
		}
	}
	return config.Config{}, false
}

// routeConfigNameFor computes the Envoy RouteConfiguration name istiod assigns
// the gateway server listening on `port`, mirroring istio's (unexported)
// gatewayRDSRouteName (pilot/pkg/model/gateway.go): "http.<port>" for a plain
// HTTP server, "https.<port>.<portName>.<gwName>.<gwNamespace>" for a
// TLS-terminated HTTPS server. ok=false when no server listens on that port or
// the server is TLS passthrough (no HTTP RDS route) — both a miss. port<=0 => 80.
func routeConfigNameFor(gwCfg config.Config, port int) (string, bool) {
	if port <= 0 {
		port = 80
	}
	gw, _ := gwCfg.Spec.(*networking.Gateway)
	if gw == nil {
		return "", false
	}
	for _, s := range gw.Servers {
		if s.GetPort() == nil || int(s.Port.Number) != port {
			continue
		}
		p := protocol.Parse(s.Port.Protocol)
		switch {
		case p.IsHTTP():
			return fmt.Sprintf("http.%d", port), true
		case p == protocol.HTTPS && !gateway.IsPassThroughServer(s):
			return fmt.Sprintf("https.%d.%s.%s.%s", s.Port.Number, s.Port.Name, gwCfg.Name, gwCfg.Namespace), true
		}
		return "", false // matched port, but passthrough/TCP/TLS: no HTTP RDS route
	}
	return "", false
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
