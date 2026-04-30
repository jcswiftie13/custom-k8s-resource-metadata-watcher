// Command metadata-exporter is a lightweight Kubernetes metadata collector
// that watches resources via SharedInformers and publishes per-series labels
// as Prometheus `_info` gauges.
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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/example/metadata-exporter/pkg/collector"
	"github.com/example/metadata-exporter/pkg/config"
	"github.com/example/metadata-exporter/pkg/sink"
)

func main() {
	var (
		configPath  = flag.String("config", "/etc/metadata-exporter/config.yaml", "Path to YAML config file")
		metricsAddr = flag.String("metrics-addr", ":8080", "Address to serve /metrics on")
		kubeconfig  = flag.String("kubeconfig", "", "Path to kubeconfig (empty = in-cluster)")
		logLevel    = flag.String("log-level", "info", "Log level: debug | info | warn | error")
		kubeQPS     = flag.Float64("kube-api-qps", 20, "Maximum QPS the kubernetes client issues against the apiserver")
		kubeBurst   = flag.Int("kube-api-burst", 40, "Maximum burst the kubernetes client issues against the apiserver")
		workers     = flag.Int("reconcile-workers", 4, "Number of goroutines that drain the reconcile workqueue")
	)
	flag.Parse()

	log := newLogger(*logLevel)
	slog.SetDefault(log)

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("load config failed", "err", err)
		os.Exit(1)
	}
	log.Info("config loaded", "rules", len(cfg.Rules), "watchKinds", cfg.Watch.EffectiveKinds(), "watchResources", cfg.Watch.EffectiveResources())

	restCfg, err := buildRestConfig(*kubeconfig)
	if err != nil {
		log.Error("build kube client config failed", "err", err)
		os.Exit(1)
	}
	restCfg.QPS = float32(*kubeQPS)
	restCfg.Burst = *kubeBurst
	log.Info("kube client configured", "qps", restCfg.QPS, "burst", restCfg.Burst)
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Error("kubernetes client failed", "err", err)
		os.Exit(1)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerClientGoMetrics(reg)
	ps := sink.NewPrometheusSink(reg)

	col, err := collector.New(cfg, client, ps, log, collector.Options{
		Workers:    *workers,
		Registerer: reg,
	})
	if err != nil {
		log.Error("collector init failed", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

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
