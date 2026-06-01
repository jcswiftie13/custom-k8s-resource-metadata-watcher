// Package config loads and validates the metadata-exporter YAML configuration.
//
// The config model is rule-based: each rule describes one Prometheus metric
// whose subject (the "anchor") is a Kubernetes resource kind, and whose labels
// are expressed as JSONPath extractions over the anchor object or related
// objects resolved through ownerReferences. Optionally, a rule may also
// declare expandLabels which flatten Kubernetes maps (typically
// metadata.labels / metadata.annotations) into dynamic Prometheus label
// names at scrape time.
package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"sigs.k8s.io/yaml"
)

// Supported anchor kinds in v1.
var supportedAnchors = map[string]struct{}{
	"Pod":         {},
	"Deployment":  {},
	"StatefulSet": {},
	"DaemonSet":   {},
	"ReplicaSet":  {},
	"Node":        {},
}

// allSupportedKindOrder is the fixed order used for informers, validation, and
// log output when watch.kinds is empty (watch all) or for merging explicit kinds.
var allSupportedKindOrder = []string{
	"Pod", "ReplicaSet", "Deployment", "StatefulSet", "DaemonSet", "Node",
}

// Built-in source names recognised on labels.source / expandLabels.source
// (case-sensitive). Kind names are also accepted as sources (first occurrence
// along the owner chain).
var builtinSources = map[string]struct{}{
	"anchor":          {},
	"item":            {},
	"ownerController": {},
	"topController":   {},
}

var (
	promLabelNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	metricNameRe    = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
)

// Config is the root configuration model.
type Config struct {
	// MetricPrefix is prepended to every rule's name when registering the metric.
	// Example: MetricPrefix="custom_" + rule.Name="pod_info" => "custom_pod_info".
	MetricPrefix string `json:"metricPrefix,omitempty"`

	// Watch narrows the informer scope to reduce apiserver load.
	Watch WatchScope `json:"watch,omitempty"`

	// Rules declares the set of metrics to export.
	Rules []Rule `json:"rules"`
}

// WatchScope describes which kinds to watch, optional namespace limits, and
// per-kind list/watch filters.
type WatchScope struct {
	// Resources declares per-kind watch conditions. Empty means watch all
	// supported resources with default scopes/selectors.
	Resources []WatchResource `json:"resources,omitempty"`
}

// WatchResource holds list/watch options and scope for one resource kind.
type WatchResource struct {
	Kind string `json:"kind"`
	// Scope must be "Namespaced" or "Cluster".
	Scope string `json:"scope"`
	// Namespaces is only valid for namespaced resources when scope=Namespaced.
	Namespaces    []string `json:"namespaces,omitempty"`
	LabelSelector string   `json:"labelSelector,omitempty"`
	FieldSelector string   `json:"fieldSelector,omitempty"`
}

const (
	ScopeNamespaced = "Namespaced"
	ScopeCluster    = "Cluster"
)

// EffectiveResources returns watched resources in stable kind order.
// Empty Resources means every supported kind is watched with defaults.
func (w WatchScope) EffectiveResources() []WatchResource {
	if len(w.Resources) == 0 {
		out := make([]WatchResource, 0, len(allSupportedKindOrder))
		for _, k := range allSupportedKindOrder {
			out = append(out, WatchResource{
				Kind:  k,
				Scope: defaultScopeForKind(k),
			})
		}
		return out
	}
	byKind := make(map[string]WatchResource, len(w.Resources))
	for _, r := range w.Resources {
		byKind[r.Kind] = r
	}
	var out []WatchResource
	for _, k := range allSupportedKindOrder {
		if r, ok := byKind[k]; ok {
			out = append(out, r)
		}
	}
	return out
}

// EffectiveKinds returns the kinds to watch, in a stable order.
func (w WatchScope) EffectiveKinds() []string {
	var out []string
	for _, r := range w.EffectiveResources() {
		out = append(out, r.Kind)
	}
	return out
}

// EffectiveKindSet is a set view of EffectiveKinds.
func (w WatchScope) EffectiveKindSet() map[string]struct{} {
	eff := w.EffectiveKinds()
	m := make(map[string]struct{}, len(eff))
	for _, k := range eff {
		m[k] = struct{}{}
	}
	return m
}

// ResourceFor returns the effective watch resource for a kind.
func (w WatchScope) ResourceFor(kind string) (WatchResource, bool) {
	for _, r := range w.EffectiveResources() {
		if r.Kind == kind {
			return r, true
		}
	}
	return WatchResource{}, false
}

// Rule is one Prometheus metric declaration.
type Rule struct {
	// Name, combined with Config.MetricPrefix, yields the Prometheus metric name.
	Name string `json:"name"`

	// Help is the Prometheus metric help string.
	Help string `json:"help,omitempty"`

	// Anchor is the Kubernetes Kind whose objects produce the series.
	Anchor string `json:"anchor"`

	// ForEach is an optional JSONPath (relative to anchor) evaluating to a
	// list; one series is emitted per element.
	ForEach string `json:"forEach,omitempty"`

	// Labels maps Prometheus label name -> Extract producing its value. The
	// label name set declared here is the FIXED part of every series for
	// this rule. ExpandLabels add dynamic, per-series label keys on top.
	Labels map[string]Extract `json:"labels,omitempty"`

	// ExpandLabels declares any number of "flatten this map into dynamic
	// label names" rules. Each entry reads a Kubernetes map (e.g.
	// metadata.labels) and, after key sanitisation and prefixing, every
	// (k,v) pair becomes a Prometheus label on that anchor's series.
	// Different anchors may carry different dynamic keys in the same scrape.
	ExpandLabels []ExpandLabel `json:"expandLabels,omitempty"`
}

// Extract describes how to produce a single string value.
type Extract struct {
	// Source is the name of the object to evaluate Path against.
	// Defaults to "anchor" when empty.
	Source string `json:"source,omitempty"`

	// Path is a kubectl-inspired path expression; for example,
	// `metadata.annotations["example.com/foo"]` or `spec.containers[*].image`.
	// See docs/CONFIG.md for the supported grammar subset.
	Path string `json:"path,omitempty"`

	// Fallbacks are tried in order when the primary extract resolves to an
	// empty / missing value. Fallbacks may not nest further fallbacks.
	Fallbacks []Extract `json:"fallbacks,omitempty"`

	// OnMissing is returned when all of Path and Fallbacks miss. When nil,
	// an empty string is used (preserving a fixed label set).
	OnMissing *string `json:"onMissing,omitempty"`
}

// OnMissingValue returns the configured fallback string (defaulting to "").
func (e Extract) OnMissingValue() string {
	if e.OnMissing == nil {
		return ""
	}
	return *e.OnMissing
}

// EffectiveSource returns the source name after applying the "anchor" default.
func (e Extract) EffectiveSource() string {
	if e.Source == "" {
		return "anchor"
	}
	return e.Source
}

// ExpandLabel flattens a Kubernetes map into dynamic Prometheus labels.
//
// Example: source="anchor", path="metadata.labels", prefix="label_" turns
//
//	metadata.labels{ "app.kubernetes.io/name": "api", "team": "payments" }
//
// into
//
//	label_app_kubernetes_io_name="api", label_team="payments"
//
// on every series produced by the rule for that anchor.
type ExpandLabel struct {
	// Source identifies the related object whose Path resolves to a map.
	// Defaults to "anchor". Same naming convention as Extract.Source.
	Source string `json:"source,omitempty"`

	// Path resolves to the map to flatten. Most common values:
	// "metadata.labels", "metadata.annotations".
	Path string `json:"path"`

	// Prefix is prepended to every dynamic label name. It must form a valid
	// Prometheus label name on its own (matching [a-zA-Z_][a-zA-Z0-9_]*).
	// Common conventional choices: "label_", "annotation_".
	Prefix string `json:"prefix"`

	// Allow optionally narrows the set of map keys exported. When non-empty
	// only keys present in Allow are emitted.
	Allow []string `json:"allow,omitempty"`

	// Deny is a blacklist of map keys to skip. Deny wins over Allow when a
	// key appears in both.
	Deny []string `json:"deny,omitempty"`

	// MaxKeys caps the number of dynamic keys emitted per anchor (per
	// ExpandLabel entry). 0 means no limit. When non-zero the cheapest
	// stable bound is achieved by sorting key candidates lexicographically
	// and dropping the tail.
	MaxKeys int `json:"maxKeys,omitempty"`
}

// EffectiveSource returns the source name after applying the "anchor" default.
func (e ExpandLabel) EffectiveSource() string {
	if e.Source == "" {
		return "anchor"
	}
	return e.Source
}

// Load reads and validates a config file from disk.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %q: %w", path, err)
	}
	return cfg, nil
}

// Validate performs structural checks. The caller is expected to run a
// separate pass that compiles every JSONPath (see pkg/collector/evaluator).
func (c *Config) Validate() error {
	if len(c.Rules) == 0 {
		return fmt.Errorf("rules: at least one rule is required")
	}
	if c.MetricPrefix != "" && !metricNameRe.MatchString(c.MetricPrefix+"x") {
		return fmt.Errorf("metricPrefix %q is not a valid Prometheus metric name prefix", c.MetricPrefix)
	}
	if err := c.validateWatchKinds(); err != nil {
		return err
	}
	seenMetric := map[string]struct{}{}
	for i := range c.Rules {
		r := &c.Rules[i]
		if err := r.validate(); err != nil {
			return fmt.Errorf("rules[%d] (name=%q): %w", i, r.Name, err)
		}
		if err := r.validateAgainstWatch(c.Watch); err != nil {
			return fmt.Errorf("rules[%d] (name=%q): %w", i, r.Name, err)
		}
		metricName := c.MetricPrefix + r.Name
		if !metricNameRe.MatchString(metricName) {
			return fmt.Errorf("rules[%d]: metric name %q is not a valid Prometheus metric name", i, metricName)
		}
		if _, dup := seenMetric[metricName]; dup {
			return fmt.Errorf("rules[%d]: metric name %q is duplicated across rules", i, metricName)
		}
		seenMetric[metricName] = struct{}{}
	}
	return nil
}

// validateWatchKinds checks watch.resources schema and kind/scope compatibility.
func (c *Config) validateWatchKinds() error {
	seen := map[string]struct{}{}
	for i, r := range c.Watch.Resources {
		if _, ok := supportedAnchors[r.Kind]; !ok {
			return fmt.Errorf("watch.resources[%d].kind: unknown kind %q (allowed: Pod, Deployment, StatefulSet, DaemonSet, ReplicaSet, Node)", i, r.Kind)
		}
		if _, dup := seen[r.Kind]; dup {
			return fmt.Errorf("watch.resources[%d].kind: duplicated kind %q", i, r.Kind)
		}
		seen[r.Kind] = struct{}{}
		if r.Scope != ScopeNamespaced && r.Scope != ScopeCluster {
			return fmt.Errorf("watch.resources[%d].scope: must be %q or %q", i, ScopeNamespaced, ScopeCluster)
		}
		if r.Kind == "Node" {
			if r.Scope != ScopeCluster {
				return fmt.Errorf("watch.resources[%d]: kind %q must use scope=%q", i, r.Kind, ScopeCluster)
			}
			if len(r.Namespaces) > 0 {
				return fmt.Errorf("watch.resources[%d]: kind %q cannot set namespaces (cluster-scoped resource)", i, r.Kind)
			}
			continue
		}
		if r.Scope == ScopeCluster && len(r.Namespaces) > 0 {
			return fmt.Errorf("watch.resources[%d]: scope=%q cannot set namespaces", i, ScopeCluster)
		}
	}
	return nil
}

// requiredWatchKinds are kinds the rule's anchor and explicit label/expandLabel
// sources need to have in the informer cache (excludes ownerController/topController).
func (r *Rule) requiredWatchKinds() map[string]struct{} {
	out := map[string]struct{}{}
	out[r.Anchor] = struct{}{}
	add := func(src string) {
		switch src {
		case "", "anchor", "item", "ownerController", "topController":
			return
		}
		if _, ok := supportedAnchors[src]; ok {
			out[src] = struct{}{}
		}
	}
	for _, ext := range r.Labels {
		add(ext.EffectiveSource())
		for _, f := range ext.Fallbacks {
			add(f.EffectiveSource())
		}
	}
	for _, ex := range r.ExpandLabels {
		add(ex.EffectiveSource())
	}
	return out
}

func (r *Rule) validateAgainstWatch(w WatchScope) error {
	eff := w.EffectiveKindSet()
	for k := range r.requiredWatchKinds() {
		if _, ok := eff[k]; !ok {
			return fmt.Errorf("kind %q is required by anchor/sources but is not included in watch.resources (effective watch set does not include it)", k)
		}
	}
	return nil
}

// validSourceSet returns the set of legal source names recognised by labels
// and expandLabels: built-ins plus every supported Kind name. Relation
// aliases are no longer supported; refer to topController / ownerController
// or a kind name directly.
func validSourceSet() map[string]struct{} {
	out := make(map[string]struct{}, len(builtinSources)+len(supportedAnchors))
	for k, v := range builtinSources {
		out[k] = v
	}
	for k := range supportedAnchors {
		out[k] = struct{}{}
	}
	return out
}

func (r *Rule) validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return fmt.Errorf("name: required")
	}
	if _, ok := supportedAnchors[r.Anchor]; !ok {
		return fmt.Errorf("anchor: %q is not supported (allowed: Pod, Deployment, StatefulSet, DaemonSet, ReplicaSet, Node)", r.Anchor)
	}
	if len(r.Labels) == 0 && len(r.ExpandLabels) == 0 {
		return fmt.Errorf("labels: at least one fixed label or expandLabels entry is required")
	}

	validSources := validSourceSet()

	// Validate forEach: only meaningful when the anchor has array fields.
	if r.ForEach != "" {
		if r.Anchor == "" {
			return fmt.Errorf("forEach: requires a valid anchor")
		}
	}

	hasForEach := r.ForEach != ""

	fixedNames := map[string]struct{}{}
	for name, extract := range r.Labels {
		if !promLabelNameRe.MatchString(name) {
			return fmt.Errorf("labels[%q]: invalid Prometheus label name (must match [a-zA-Z_][a-zA-Z0-9_]*)", name)
		}
		if strings.HasPrefix(name, "__") {
			return fmt.Errorf("labels[%q]: names starting with __ are reserved", name)
		}
		if err := extract.validate(validSources, hasForEach); err != nil {
			return fmt.Errorf("labels[%q]: %w", name, err)
		}
		fixedNames[name] = struct{}{}
	}

	for i, ex := range r.ExpandLabels {
		if err := ex.validate(validSources, hasForEach); err != nil {
			return fmt.Errorf("expandLabels[%d]: %w", i, err)
		}
		// A prefix that exactly matches a fixed label name would create
		// ambiguity for keys that sanitise to "" — not common, but warn
		// loudly by failing fast.
		if _, dup := fixedNames[ex.Prefix]; dup {
			return fmt.Errorf("expandLabels[%d]: prefix %q collides with a fixed label name", i, ex.Prefix)
		}
	}
	return nil
}

func (e Extract) validate(validSources map[string]struct{}, forEachActive bool) error {
	src := e.EffectiveSource()
	if _, ok := validSources[src]; !ok {
		return fmt.Errorf("source %q is not recognised", src)
	}
	if src == "item" && !forEachActive {
		return fmt.Errorf("source=item requires forEach on the rule")
	}
	if strings.TrimSpace(e.Path) == "" {
		return fmt.Errorf("path: required")
	}
	for i, f := range e.Fallbacks {
		if len(f.Fallbacks) > 0 {
			return fmt.Errorf("fallbacks[%d]: nested fallbacks are not supported", i)
		}
		if err := f.validate(validSources, forEachActive); err != nil {
			return fmt.Errorf("fallbacks[%d]: %w", i, err)
		}
	}
	return nil
}

func (e ExpandLabel) validate(validSources map[string]struct{}, forEachActive bool) error {
	src := e.EffectiveSource()
	if _, ok := validSources[src]; !ok {
		return fmt.Errorf("source %q is not recognised", src)
	}
	if src == "item" && !forEachActive {
		return fmt.Errorf("source=item requires forEach on the rule")
	}
	if strings.TrimSpace(e.Path) == "" {
		return fmt.Errorf("path: required")
	}
	if strings.TrimSpace(e.Prefix) == "" {
		return fmt.Errorf("prefix: required (e.g. label_, annotation_)")
	}
	// Validate that the prefix can begin a Prometheus label name. We
	// approximate by appending an arbitrary identifier-safe suffix.
	if !promLabelNameRe.MatchString(e.Prefix + "x") {
		return fmt.Errorf("prefix %q does not form a valid Prometheus label name", e.Prefix)
	}
	if strings.HasPrefix(e.Prefix, "__") {
		return fmt.Errorf("prefix %q is reserved (must not begin with __)", e.Prefix)
	}
	if e.MaxKeys < 0 {
		return fmt.Errorf("maxKeys: must be >= 0 (0 means unlimited)")
	}
	return nil
}

// MetricName returns the fully-qualified Prometheus metric name for a rule.
func (c *Config) MetricName(r *Rule) string {
	return c.MetricPrefix + r.Name
}

func defaultScopeForKind(kind string) string {
	if kind == "Node" {
		return ScopeCluster
	}
	return ScopeNamespaced
}
