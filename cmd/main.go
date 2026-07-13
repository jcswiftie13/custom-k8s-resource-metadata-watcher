// Command metadata-exporter is a lightweight Kubernetes metadata collector
// that watches resources via SharedInformers and publishes per-series labels
// as Prometheus `_info` gauges. It is implemented as a custom prometheus
// Collector, so each /metrics scrape walks the informer caches at scrape
// time and emits one constant-value series per (anchor, forEach-item).
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/example/metadata-exporter/pkg/collector"
	"github.com/example/metadata-exporter/pkg/config"
	"github.com/example/metadata-exporter/pkg/history"
	"github.com/example/metadata-exporter/pkg/store"
)

func main() {
	var (
		configPath  = flag.String("config", "/etc/metadata-exporter/config.yaml", "Path to YAML config file")
		metricsAddr = flag.String("metrics-addr", ":8080", "Address to serve /metrics on")
		kubeconfig  = flag.String("kubeconfig", "", "Path to kubeconfig (empty = in-cluster)")
		logLevel    = flag.String("log-level", "info", "Log level: debug | info | warn | error")
		kubeQPS     = flag.Float64("kube-api-qps", 20, "Maximum QPS the kubernetes client issues against the apiserver")
		kubeBurst   = flag.Int("kube-api-burst", 40, "Maximum burst the kubernetes client issues against the apiserver")
	)
	flag.Parse()

	log := newLogger(*logLevel)
	slog.SetDefault(log)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config failed", "err", err)
		os.Exit(1)
	}
	log.Info("config parsed", "rules", len(cfg.Rules), "discoveryEnabled", cfg.Discovery.Enabled)

	restCfg, err := buildRestConfig(*kubeconfig)
	if err != nil {
		log.Error("build kube client config failed", "err", err)
		os.Exit(1)
	}
	restCfg.QPS = float32(*kubeQPS)
	restCfg.Burst = *kubeBurst
	log.Info("kube client configured", "qps", restCfg.QPS, "burst", restCfg.Burst)
	client, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		log.Error("dynamic kubernetes client failed", "err", err)
		os.Exit(1)
	}
	if cfg.Discovery.Enabled {
		disco, err := discovery.NewDiscoveryClientForConfig(restCfg)
		if err != nil {
			log.Error("discovery client failed", "err", err)
			os.Exit(1)
		}
		if err := cfg.ResolveDiscovery(disco); err != nil {
			log.Error("discovery resolution failed", "err", err)
			os.Exit(1)
		}
	}
	registry, err := cfg.Registry()
	if err != nil {
		log.Error("resource registry build failed", "err", err)
		os.Exit(1)
	}
	log.Info("config loaded", "rules", len(cfg.Rules), "watchResources", registry.Resources())

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerClientGoMetrics(reg)

	col, err := collector.New(cfg, client, log, collector.Options{
		Registerer: reg,
	})
	if err != nil {
		log.Error("collector init failed", "err", err)
		os.Exit(1)
	}
	reg.MustRegister(col)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// History ingest is fully opt-in and decoupled from the scrape path. It
	// registers event handlers on the SAME informer cache the collector owns,
	// so it must be wired before col.Start (which starts the informers).
	if cfg.History.Enabled {
		compiled, err := history.CompileAll(cfg.History)
		if err != nil {
			log.Error("history compile failed", "err", err)
			os.Exit(1)
		}
		sc := cfg.History.Store
		user, err := sc.ResolveUsername()
		if err != nil {
			log.Error("history store username resolve failed", "err", err)
			os.Exit(1)
		}
		pw, err := sc.ResolvePassword()
		if err != nil {
			log.Error("history store password resolve failed", "err", err)
			os.Exit(1)
		}
		tok, err := sc.ResolveToken()
		if err != nil {
			log.Error("history store token resolve failed", "err", err)
			os.Exit(1)
		}
		histStore, err := store.Open(ctx, store.Options{
			DSN:           sc.DSN,
			Username:      user,
			Password:      pw,
			Database:      sc.Database,
			Token:         tok,
			Secure:        sc.SecureEnabled(),
			TLSSkipVerify: sc.TLSSkipVerify,
			CreateSchema:  sc.CreateSchemaEnabled(),
			UpdateClose:   sc.CloseModeOrDefault() == config.CloseModeUpdate,
		})
		if err != nil {
			log.Error("history store open failed", "err", err)
			os.Exit(1)
		}
		defer func() { _ = histStore.Close() }()
		if err := histStore.EnsureSchema(ctx, history.TableSchemas(compiled)); err != nil {
			log.Error("history schema ensure failed", "err", err)
			os.Exit(1)
		}
		ing := history.NewIngester(histStore, col.Informers(), compiled, cfg.History.Store.Batch,
			cfg.History.Store.CloseModeOrDefault(), log)
		// Restart idempotency (closeMode=update): rebuild last-state from the
		// store's open rows BEFORE the informers start, so the initial re-LIST
		// dedups against what was already written instead of re-inserting it.
		if err := ing.Recover(ctx); err != nil {
			log.Error("history last-state recovery failed", "err", err)
			os.Exit(1)
		}
		if err := ing.Register(); err != nil {
			log.Error("history handler register failed", "err", err)
			os.Exit(1)
		}
		ing.Start(ctx)
		log.Info("history ingest enabled", "resources", len(compiled),
			"createSchema", cfg.History.Store.CreateSchemaEnabled(),
			"closeMode", cfg.History.Store.CloseModeOrDefault())
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	server := &http.Server{
		Addr:              *metricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Info("metrics server listening", "addr", *metricsAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	collectorErr := make(chan error, 1)
	go func() {
		if err := col.Start(ctx); err != nil {
			collectorErr <- err
		}
		close(collectorErr)
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown requested")
	case err := <-serverErr:
		log.Error("http server error", "err", err)
		cancel()
	case err := <-collectorErr:
		if err != nil {
			log.Error("collector error", "err", err)
			cancel()
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
}

func buildRestConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	return slog.New(h)
}
