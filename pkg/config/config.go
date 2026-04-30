// Package config loads and validates the metadata-exporter YAML configuration.
//
// The config model is rule-based: each rule describes one Prometheus metric
// whose subject (the "anchor") is a Kubernetes resource kind, and whose labels
// are expressed as JSONPath extractions over the anchor object or related
// objects resolved through ownerReferences.
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

// Built-in relation source names (case-sensitive).
// Kind names are also accepted as sources (first occurrence along the owner chain).
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
	Namespaces []string `json:"namespaces,omitempty"`
	LabelSelector string `json:"labelSelector,omitempty"`
	FieldSelector string `json:"fieldSelector,omitempty"`
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

	// Anchor is the Kubernetes Kind whose events trigger reconciliation and
	// which is the subject of each emitted series.
	Anchor string `json:"anchor"`

	// ForEach is an optional JSONPath (relative to anchor) evaluating to a
	// list; one series is emitted per element.
	ForEach string `json:"forEach,omitempty"`

	// Relations declares named aliases for related objects (e.g. topController).
	Relations []RelationAlias `json:"relations,omitempty"`

	// Labels maps Prometheus label name -> Extract producing its value.
	Labels map[string]Extract `json:"labels"`

	// Flatten expands map-valued fields (e.g. metadata.annotations or
	// metadata.labels) into a fixed set of additional Prometheus labels
	// using an explicit allow-list of keys. Each entry contributes
	// len(Keys) synthetic labels on top of Labels.
	Flatten []FlattenExtract `json:"flatten,omitempty"`
}

// FlattenExtract declares an allow-list of keys to pull out of a map-valued
// path on a given source, producing one Prometheus label per key. The
// resulting label names are formed as NamePrefix + SanitizeLabelName(key),
// and must not collide with any entry in Rule.Labels or other flatten
// expansions within the same rule.
type FlattenExtract struct {
	// NamePrefix is prepended verbatim to every generated label name.
	// Defaults to "". The final name must still satisfy the Prometheus
	// label-name grammar ([a-zA-Z_][a-zA-Z0-9_]*) and must not start
	// with "__".
	NamePrefix string `json:"namePrefix,omitempty"`

	// Source names the object to evaluate Path against. Defaults to
	// "anchor". Follows the same rules as Extract.Source.
	Source string `json:"source,omitempty"`

	// Path must resolve to a map[string]interface{} (e.g.
	// "metadata.annotations" or "metadata.labels"). Paths that resolve
	// to anything else are treated as a total miss and OnMissing is used.
	Path string `json:"path"`

	// Keys is the non-empty list of map keys to extract. Keys may contain
	// characters that are illegal in Prometheus label names (e.g. '.',
	// '/', '-'); those are replaced with '_' when forming the label name.
	Keys []string `json:"keys"`

	// OnMissing is returned when a key is absent. When nil, the empty
	// string is used (consistent with the existing Extract semantics).
	OnMissing *string `json:"onMissing,omitempty"`
}

// EffectiveSource returns the source name after applying the "anchor" default.
func (f FlattenExtract) EffectiveSource() string {
	if f.Source == "" {
		return "anchor"
	}
	return f.Source
}

// OnMissingValue returns the configured fallback string (defaulting to "").
func (f FlattenExtract) OnMissingValue() string {
	if f.OnMissing == nil {
		return ""
	}
	return *f.OnMissing
}

// SanitizeLabelName coerces an arbitrary string into a valid Prometheus
// label name. Any character outside [A-Za-z0-9_] is replaced with '_', and
// if the first rune is not a letter or underscore (e.g. it is a digit, or
// a symbol that was just rewritten to '_' but happened to be a digit) the
// result is prefixed with '_'. Callers are still expected to prepend a
// namePrefix and re-check against promLabelNameRe.
func SanitizeLabelName(s string) string {
	if s == "" {
		return ""
	}
	out := make([]rune, 0, len(s)+1)
	first := true
	for _, r := range s {
		legal := r == '_' ||
			(r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9')
		if !legal {
			r = '_'
		}
		if first {
			if r >= '0' && r <= '9' {
				out = append(out, '_')
			}
			first = false
		}
		out = append(out, r)
	}
	return string(out)
}

// RelationAlias gives a short local name to a source.
type RelationAlias struct {
	Name string `json:"name"`
	Via  string `json:"via"`
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

// Load reads and validates a config file from disk.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var doc map[string]interface{}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	if err := rejectLegacyWatchSelectors(path, doc); err != nil {
		return nil, err
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

func rejectLegacyWatchSelectors(path string, doc map[string]interface{}) error {
	w, ok := doc["watch"]
	if !ok || w == nil {
		return nil
	}
	wmap, ok := w.(map[string]interface{})
	if !ok {
		return nil
	}
	if _, has := wmap["selectors"]; has {
		return fmt.Errorf("config %q: watch.selectors is no longer supported; use watch.resources (see docs/CONFIG.md)", path)
	}
	if _, has := wmap["kinds"]; has {
		return fmt.Errorf("config %q: watch.kinds is no longer supported in v2; use watch.resources", path)
	}
	if _, has := wmap["namespaces"]; has {
		return fmt.Errorf("config %q: watch.namespaces is no longer supported in v2; use watch.resources[].namespaces", path)
	}
	return nil
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

// requiredWatchKinds are kinds the rule's anchor and explicit label/flatten
// sources need to have in the informer cache (excludes ownerController/topController).
func (r *Rule) requiredWatchKinds() map[string]struct{} {
	out := map[string]struct{}{}
	out[r.Anchor] = struct{}{}
	add := func(src string) {
		resolved := r.ResolveRelation(src)
		switch resolved {
		case "", "anchor", "item", "ownerController", "topController":
			return
		}
		if _, ok := supportedAnchors[resolved]; ok {
			out[resolved] = struct{}{}
		}
	}
	for _, ext := range r.Labels {
		add(ext.EffectiveSource())
		for _, f := range ext.Fallbacks {
			add(f.EffectiveSource())
		}
	}
	for _, fl := range r.Flatten {
		add(fl.EffectiveSource())
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

func (r *Rule) validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return fmt.Errorf("name: required")
	}
	if _, ok := supportedAnchors[r.Anchor]; !ok {
		return fmt.Errorf("anchor: %q is not supported (allowed: Pod, Deployment, StatefulSet, DaemonSet, ReplicaSet)", r.Anchor)
	}
	if len(r.Labels) == 0 {
		return fmt.Errorf("labels: at least one label is required")
	}

	// Build the set of valid source names: builtins + kinds + relation aliases.
	validSources := map[string]struct{}{}
	for k, v := range builtinSources {
		validSources[k] = v
	}
	for k := range supportedAnchors {
		validSources[k] = struct{}{}
	}
	for i, rel := range r.Relations {
		if strings.TrimSpace(rel.Name) == "" {
			return fmt.Errorf("relations[%d].name: required", i)
		}
		if _, ok := builtinSources[rel.Via]; !ok {
			if _, ok := supportedAnchors[rel.Via]; !ok {
				return fmt.Errorf("relations[%d]: via=%q is not a valid source (allowed: anchor, ownerController, topController, or a supported kind)", i, rel.Via)
			}
		}
		if rel.Via == "item" {
			return fmt.Errorf("relations[%d]: via=%q is not permitted (item is only valid as a direct source on a label when forEach is set)", i, rel.Via)
		}
		if _, dup := validSources[rel.Name]; dup {
			return fmt.Errorf("relations[%d]: name=%q collides with a builtin or earlier alias", i, rel.Name)
		}
		validSources[rel.Name] = struct{}{}
	}

	// Validate forEach: only meaningful when the anchor has array fields.
	if r.ForEach != "" {
		// Allowed anchors for forEach: keep permissive but sane.
		// Evaluator will reject unparseable paths.
		if r.Anchor == "" {
			return fmt.Errorf("forEach: requires a valid anchor")
		}
	}

	hasForEach := r.ForEach != ""
	seenLabelName := make(map[string]struct{}, len(r.Labels)+len(r.Flatten))
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
		seenLabelName[name] = struct{}{}
	}

	for i, f := range r.Flatten {
		if err := f.validate(validSources, hasForEach); err != nil {
			return fmt.Errorf("flatten[%d]: %w", i, err)
		}
		for j, key := range f.Keys {
			name := f.NamePrefix + SanitizeLabelName(key)
			if !promLabelNameRe.MatchString(name) {
				return fmt.Errorf("flatten[%d].keys[%d]: key %q produces invalid Prometheus label name %q", i, j, key, name)
			}
			if strings.HasPrefix(name, "__") {
				return fmt.Errorf("flatten[%d].keys[%d]: key %q produces label name %q starting with __ (reserved)", i, j, key, name)
			}
			if _, dup := seenLabelName[name]; dup {
				return fmt.Errorf("flatten[%d].keys[%d]: key %q produces label name %q that collides with an existing label", i, j, key, name)
			}
			seenLabelName[name] = struct{}{}
		}
	}
	return nil
}

func (f FlattenExtract) validate(validSources map[string]struct{}, forEachActive bool) error {
	src := f.EffectiveSource()
	if _, ok := validSources[src]; !ok {
		return fmt.Errorf("source %q is not recognised", src)
	}
	if src == "item" && !forEachActive {
		return fmt.Errorf("source=item requires forEach on the rule")
	}
	if strings.TrimSpace(f.Path) == "" {
		return fmt.Errorf("path: required")
	}
	if len(f.Keys) == 0 {
		return fmt.Errorf("keys: at least one key is required")
	}
	seenKey := make(map[string]struct{}, len(f.Keys))
	for i, k := range f.Keys {
		if strings.TrimSpace(k) == "" {
			return fmt.Errorf("keys[%d]: must be a non-empty string", i)
		}
		if _, dup := seenKey[k]; dup {
			return fmt.Errorf("keys[%d]: duplicated key %q", i, k)
		}
		seenKey[k] = struct{}{}
	}
	return nil
}

func (e Extract) validate(validSources map[string]struct{}, forEachActive bool) error {
	src := e.Source
	if src == "" {
		src = "anchor"
	}
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

// MetricName returns the fully-qualified Prometheus metric name for a rule.
func (c *Config) MetricName(r *Rule) string {
	return c.MetricPrefix + r.Name
}

// EffectiveSource returns the source name after applying the "anchor" default.
func (e Extract) EffectiveSource() string {
	if e.Source == "" {
		return "anchor"
	}
	return e.Source
}

// ResolveRelation returns the underlying built-in source (or kind) that a
// relation alias points to. Builtin names and kinds pass through unchanged.
// Unknown names return the input as-is so callers can still surface a clear
// error from evaluation.
func (r *Rule) ResolveRelation(name string) string {
	for _, rel := range r.Relations {
		if rel.Name == name {
			return rel.Via
		}
	}
	return name
}

func defaultScopeForKind(kind string) string {
	if kind == "Node" {
		return ScopeCluster
	}
	return ScopeNamespaced
}
