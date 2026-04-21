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

// WatchScope describes namespace and per-kind selector restrictions.
type WatchScope struct {
	// Namespaces, if non-empty, limits informers to these namespaces; one
	// SharedInformerFactory is created per namespace.
	Namespaces []string `json:"namespaces,omitempty"`

	// Selectors holds per-kind label/field selectors applied via
	// WithTweakListOptions. The key is the Kind (Pod/Deployment/...).
	Selectors map[string]KindSelector `json:"selectors,omitempty"`
}

// KindSelector is a label/field selector pair applied to a specific kind.
type KindSelector struct {
	LabelSelector string `json:"labelSelector,omitempty"`
	FieldSelector string `json:"fieldSelector,omitempty"`
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
	seenMetric := map[string]struct{}{}
	for i := range c.Rules {
		r := &c.Rules[i]
		if err := r.validate(); err != nil {
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
	aliasVia := map[string]string{}
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
		aliasVia[rel.Name] = rel.Via
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
