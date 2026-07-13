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
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"
)

// allSupportedKindOrder is the legacy order used for built-in kind-only
// entries. Empty watch.resources no longer expands to this list.
var allSupportedKindOrder = []string{
	"Pod", "ReplicaSet", "Deployment", "StatefulSet", "DaemonSet", "Node", "Service", "EndpointSlice",
}

// DiscoveryConfig controls optional startup-time API discovery. Discovery is
// disabled by default so config validation remains deterministic and does not
// require live-cluster permissions unless explicitly requested.
type DiscoveryConfig struct {
	Enabled bool `json:"enabled,omitempty"`
}

// OwnerConfig controls how ownerReferences are followed for a watched resource.
type OwnerConfig struct {
	// Follow defaults to true. Use follow: false for resources that should not
	// participate in owner-chain walks even when ownerReferences point to them.
	Follow *bool `json:"follow,omitempty"`
	// TopController marks this resource as satisfying source: topController.
	TopController bool `json:"topController,omitempty"`
}

// FollowEnabled returns the owner-following policy after applying defaults.
func (o OwnerConfig) FollowEnabled() bool {
	return o.Follow == nil || *o.Follow
}

// ResourceInfo describes one Kubernetes resource watched by the exporter.
type ResourceInfo struct {
	Name string
	Kind string
	// APIVersion is the Kubernetes apiVersion string used by ownerReferences.
	APIVersion string
	Group      string
	Version    string
	Resource   string
	Scope      string
	Owner      OwnerConfig
}

// GVR returns the Kubernetes GroupVersionResource used by dynamic informers.
func (r ResourceInfo) GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: r.Group, Version: r.Version, Resource: r.Resource}
}

// GVK returns the Kubernetes GroupVersionKind for objects of this resource.
func (r ResourceInfo) GVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: r.Group, Version: r.Version, Kind: r.Kind}
}

// GroupVersion returns the Kubernetes group/version string.
func (r ResourceInfo) GroupVersion() string {
	if r.APIVersion != "" {
		return r.APIVersion
	}
	return schema.GroupVersion{Group: r.Group, Version: r.Version}.String()
}

var supportedResources = map[string]ResourceInfo{
	"Pod": {
		Name:       "Pod",
		Kind:       "Pod",
		APIVersion: "v1",
		Version:    "v1",
		Resource:   "pods",
		Scope:      ScopeNamespaced,
	},
	"ReplicaSet": {
		Name:       "ReplicaSet",
		Kind:       "ReplicaSet",
		APIVersion: "apps/v1",
		Group:      "apps",
		Version:    "v1",
		Resource:   "replicasets",
		Scope:      ScopeNamespaced,
	},
	"Deployment": {
		Name:       "Deployment",
		Kind:       "Deployment",
		APIVersion: "apps/v1",
		Group:      "apps",
		Version:    "v1",
		Resource:   "deployments",
		Scope:      ScopeNamespaced,
		Owner:      OwnerConfig{TopController: true},
	},
	"StatefulSet": {
		Name:       "StatefulSet",
		Kind:       "StatefulSet",
		APIVersion: "apps/v1",
		Group:      "apps",
		Version:    "v1",
		Resource:   "statefulsets",
		Scope:      ScopeNamespaced,
		Owner:      OwnerConfig{TopController: true},
	},
	"DaemonSet": {
		Name:       "DaemonSet",
		Kind:       "DaemonSet",
		APIVersion: "apps/v1",
		Group:      "apps",
		Version:    "v1",
		Resource:   "daemonsets",
		Scope:      ScopeNamespaced,
		Owner:      OwnerConfig{TopController: true},
	},
	"Node": {
		Name:       "Node",
		Kind:       "Node",
		APIVersion: "v1",
		Version:    "v1",
		Resource:   "nodes",
		Scope:      ScopeCluster,
	},
	"Service": {
		Name:       "Service",
		Kind:       "Service",
		APIVersion: "v1",
		Version:    "v1",
		Resource:   "services",
		Scope:      ScopeNamespaced,
	},
	"EndpointSlice": {
		Name:       "EndpointSlice",
		Kind:       "EndpointSlice",
		APIVersion: "discovery.k8s.io/v1",
		Group:      "discovery.k8s.io",
		Version:    "v1",
		Resource:   "endpointslices",
		Scope:      ScopeNamespaced,
	},
}

// SupportedResource returns metadata for a supported kind.
func SupportedResource(kind string) (ResourceInfo, bool) {
	info, ok := supportedResources[kind]
	return info, ok
}

// SupportedKinds returns all supported kind names in canonical watch order.
func SupportedKinds() []string {
	return append([]string(nil), allSupportedKindOrder...)
}

func allowedKindsString() string {
	return strings.Join(allSupportedKindOrder, ", ")
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
	resourceNameRe  = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]*$`)
	kindNameRe      = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	versionRe       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	resourceRe      = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
)

// Config is the root configuration model.
type Config struct {
	// MetricPrefix is prepended to every rule's name when registering the metric.
	// Example: MetricPrefix="custom_" + rule.Name="pod_info" => "custom_pod_info".
	MetricPrefix string `json:"metricPrefix,omitempty"`

	// Watch narrows the informer scope to reduce apiserver load.
	Watch WatchScope `json:"watch,omitempty"`

	// Discovery optionally fills missing resource/scope fields from the
	// apiserver. It is disabled by default.
	Discovery DiscoveryConfig `json:"discovery,omitempty"`

	// Rules declares the set of metrics to export.
	Rules []Rule `json:"rules"`

	// History optionally enables the event-driven ClickHouse version store.
	// It is fully decoupled from the scrape/export path; when disabled the
	// exporter behaves exactly as before.
	History History `json:"history,omitempty"`
}

// WatchScope describes which kinds to watch, optional namespace limits, and
// per-kind list/watch filters.
type WatchScope struct {
	// Resources declares resource watch conditions. Empty means watch nothing.
	Resources []WatchResource `json:"resources,omitempty"`
}

// WatchResource holds list/watch options and identity for one Kubernetes
// resource. Legacy kind-only entries are accepted for built-in resources.
type WatchResource struct {
	// Name is the logical reference used by rules[].anchor and labels[].source.
	// It defaults to Kind when omitted.
	Name string `json:"name,omitempty"`
	// APIVersion is a shortcut for group/version, e.g. "batch/v1".
	APIVersion string `json:"apiVersion,omitempty"`
	Group      string `json:"group,omitempty"`
	Version    string `json:"version,omitempty"`
	Resource   string `json:"resource,omitempty"`
	Kind       string `json:"kind"`
	// Scope must be "Namespaced" or "Cluster".
	Scope string `json:"scope"`
	// Namespaces is only valid for namespaced resources when scope=Namespaced.
	Namespaces    []string    `json:"namespaces,omitempty"`
	LabelSelector string      `json:"labelSelector,omitempty"`
	FieldSelector string      `json:"fieldSelector,omitempty"`
	Owner         OwnerConfig `json:"owner,omitempty"`
}

const (
	ScopeNamespaced = "Namespaced"
	ScopeCluster    = "Cluster"
)

// EffectiveResources returns watched resources in declaration order after
// applying legacy built-in defaults. Empty Resources means watch nothing.
func (w WatchScope) EffectiveResources() []WatchResource {
	if len(w.Resources) == 0 {
		return nil
	}
	out := make([]WatchResource, 0, len(w.Resources))
	for _, r := range w.Resources {
		out = append(out, applyLegacyDefaults(r))
	}
	return out
}

// EffectiveKinds returns the resource names to watch, in declaration order.
// The method name is retained for compatibility with older call sites.
func (w WatchScope) EffectiveKinds() []string {
	var out []string
	for _, r := range w.EffectiveResources() {
		out = append(out, resourceName(r))
	}
	return out
}

// EffectiveKindSet is a set view of EffectiveKinds. The method name is
// retained for compatibility with older call sites.
func (w WatchScope) EffectiveKindSet() map[string]struct{} {
	eff := w.EffectiveKinds()
	m := make(map[string]struct{}, len(eff))
	for _, k := range eff {
		m[k] = struct{}{}
	}
	return m
}

// ResourceFor returns the effective watch resource for a resource name or a
// unique legacy kind alias.
func (w WatchScope) ResourceFor(name string) (WatchResource, bool) {
	for _, r := range w.EffectiveResources() {
		if resourceName(r) == name || r.Kind == name {
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

// Registry is the runtime resource index built from watch.resources.
type Registry struct {
	resources   []ResourceInfo
	byName      map[string]ResourceInfo
	byGVK       map[schema.GroupVersionKind]ResourceInfo
	byGVR       map[schema.GroupVersionResource]ResourceInfo
	kindToNames map[string][]string
}

// Resources returns watched resources in declaration order.
func (r *Registry) Resources() []ResourceInfo {
	if r == nil {
		return nil
	}
	return append([]ResourceInfo(nil), r.resources...)
}

// Names returns watched resource names in declaration order.
func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	out := make([]string, 0, len(r.resources))
	for _, res := range r.resources {
		out = append(out, res.Name)
	}
	return out
}

// ResourceFor resolves a rule/source reference to a watched resource. The
// reference may be a resource name or a Kind alias only when that alias is
// unambiguous among watched resources.
func (r *Registry) ResourceFor(ref string) (ResourceInfo, error) {
	if r == nil || len(r.resources) == 0 {
		return ResourceInfo{}, fmt.Errorf("resource %q is not watched: watch.resources is empty; default is to watch nothing; add the resource to watch.resources", ref)
	}
	if info, ok := r.byName[ref]; ok {
		return info, nil
	}
	names := r.kindToNames[ref]
	switch len(names) {
	case 0:
		return ResourceInfo{}, fmt.Errorf("resource %q is not recognised in watch.resources", ref)
	case 1:
		return r.byName[names[0]], nil
	default:
		sort.Strings(names)
		return ResourceInfo{}, fmt.Errorf("resource reference %q is ambiguous across watched resources %v; use source/anchor with the resource name", ref, names)
	}
}

// ResourceForGVK resolves an ownerReference GVK to a watched resource.
func (r *Registry) ResourceForGVK(gvk schema.GroupVersionKind) (ResourceInfo, bool) {
	if r == nil {
		return ResourceInfo{}, false
	}
	info, ok := r.byGVK[gvk]
	return info, ok
}

// ResourceForGVR resolves a GVR to a watched resource.
func (r *Registry) ResourceForGVR(gvr schema.GroupVersionResource) (ResourceInfo, bool) {
	if r == nil {
		return ResourceInfo{}, false
	}
	info, ok := r.byGVR[gvr]
	return info, ok
}

// SourceNames returns built-in source names plus watched resource names and
// unambiguous Kind aliases.
func (r *Registry) SourceNames() map[string]struct{} {
	out := make(map[string]struct{}, len(builtinSources)+len(r.resources))
	for k := range builtinSources {
		out[k] = struct{}{}
	}
	for _, res := range r.resources {
		out[res.Name] = struct{}{}
		if names := r.kindToNames[res.Kind]; len(names) == 1 {
			out[res.Kind] = struct{}{}
		}
	}
	return out
}

// Registry returns the fully resolved runtime registry. It fails for discovery
// placeholders that have not been resolved yet.
func (c *Config) Registry() (*Registry, error) {
	return c.buildRegistry(false)
}

// APIResourceResolver is the subset of Kubernetes discovery used by config.
type APIResourceResolver interface {
	ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error)
}

// ResolveDiscovery fills incomplete watch resources from Kubernetes discovery.
// It performs no work unless discovery.enabled is true and at least one
// resource omits resource/scope.
func (c *Config) ResolveDiscovery(resolver APIResourceResolver) error {
	if !c.Discovery.Enabled {
		return c.Validate()
	}
	if resolver == nil {
		return fmt.Errorf("discovery.enabled=true but no discovery client is available; provide complete group/version/resource/kind/scope or disable discovery")
	}
	cache := map[string]*metav1.APIResourceList{}
	for i := range c.Watch.Resources {
		r := &c.Watch.Resources[i]
		if isCompleteResource(*r) {
			continue
		}
		if isLegacyKindOnly(*r) {
			continue
		}
		gv, err := groupVersionForResource(*r)
		if err != nil {
			return fmt.Errorf("watch.resources[%d]: discovery requires apiVersion + kind or complete group/version/resource/kind/scope: %w", i, err)
		}
		if strings.TrimSpace(r.Kind) == "" {
			return fmt.Errorf("watch.resources[%d].kind: required for discovery", i)
		}
		list, ok := cache[gv.String()]
		if !ok {
			var err error
			list, err = resolver.ServerResourcesForGroupVersion(gv.String())
			if err != nil {
				return fmt.Errorf("watch.resources[%d]: discovery lookup for apiVersion %q failed: %w; provide complete group/version/resource/kind/scope or disable discovery", i, gv.String(), err)
			}
			cache[gv.String()] = list
		}
		matches := matchingAPIResources(list.APIResources, r.Kind)
		if len(matches) == 0 {
			return fmt.Errorf("watch.resources[%d]: discovery found no resource for apiVersion=%q kind=%q; provide complete group/version/resource/kind/scope", i, gv.String(), r.Kind)
		}
		if len(matches) > 1 {
			names := make([]string, 0, len(matches))
			for _, m := range matches {
				names = append(names, m.Name)
			}
			sort.Strings(names)
			return fmt.Errorf("watch.resources[%d]: discovery found multiple resources for apiVersion=%q kind=%q: %v; provide complete resource/scope", i, gv.String(), r.Kind, names)
		}
		match := matches[0]
		if !hasVerb(match.Verbs, "list") || !hasVerb(match.Verbs, "watch") {
			return fmt.Errorf("watch.resources[%d]: discovered %s does not support list/watch verbs (verbs=%v)", i, match.Name, match.Verbs)
		}
		r.Group = gv.Group
		r.Version = gv.Version
		r.APIVersion = gv.String()
		r.Resource = match.Name
		if match.Namespaced {
			r.Scope = ScopeNamespaced
		} else {
			r.Scope = ScopeCluster
		}
	}
	_, err := c.buildRegistry(false)
	if err != nil {
		return err
	}
	return c.Validate()
}

func matchingAPIResources(resources []metav1.APIResource, kind string) []metav1.APIResource {
	var out []metav1.APIResource
	for _, r := range resources {
		if r.Kind != kind || strings.Contains(r.Name, "/") {
			continue
		}
		out = append(out, r)
	}
	return out
}

func hasVerb(verbs metav1.Verbs, want string) bool {
	for _, v := range verbs {
		if v == want {
			return true
		}
	}
	return false
}

func normalizeResource(i int, raw WatchResource, allowDiscoveryPlaceholder bool) (ResourceInfo, error) {
	r := applyLegacyDefaults(raw)
	if strings.TrimSpace(r.Kind) == "" {
		return ResourceInfo{}, fmt.Errorf("watch.resources[%d].kind: required", i)
	}
	if !kindNameRe.MatchString(r.Kind) {
		return ResourceInfo{}, fmt.Errorf("watch.resources[%d].kind: invalid kind %q", i, r.Kind)
	}
	if r.Name == "" {
		r.Name = r.Kind
	}
	if !resourceNameRe.MatchString(r.Name) {
		return ResourceInfo{}, fmt.Errorf("watch.resources[%d].name: invalid resource name %q", i, r.Name)
	}
	if r.APIVersion == "" && r.Version == "" && !isLegacyKindOnly(raw) {
		if allowDiscoveryPlaceholder && r.Kind != "" {
			return ResourceInfo{
				Name:  r.Name,
				Kind:  r.Kind,
				Owner: r.Owner,
			}, nil
		}
		return ResourceInfo{}, fmt.Errorf("watch.resources[%d]: incomplete GVR for %q and discovery.enabled=false; provide group/version/resource/kind/scope or enable discovery", i, r.Name)
	}
	gv, err := groupVersionForResource(r)
	if err != nil {
		return ResourceInfo{}, fmt.Errorf("watch.resources[%d]: %w", i, err)
	}
	r.Group = gv.Group
	r.Version = gv.Version
	r.APIVersion = gv.String()
	complete := r.Resource != "" && r.Scope != ""
	if !complete {
		if allowDiscoveryPlaceholder && r.APIVersion != "" && r.Kind != "" && !isLegacyKindOnly(raw) {
			return ResourceInfo{
				Name:       r.Name,
				Kind:       r.Kind,
				APIVersion: r.APIVersion,
				Group:      r.Group,
				Version:    r.Version,
				Owner:      r.Owner,
			}, nil
		}
		if _, legacy := SupportedResource(r.Kind); legacy {
			// applyLegacyDefaults should have filled this; reaching this branch
			// usually means the user overrode part of the identity inconsistently.
			return ResourceInfo{}, fmt.Errorf("watch.resources[%d]: incomplete legacy resource %q; provide complete group/version/resource/kind/scope", i, r.Kind)
		}
		return ResourceInfo{}, fmt.Errorf("watch.resources[%d]: incomplete GVR for %q and discovery.enabled=false; provide group/version/resource/kind/scope or enable discovery", i, r.Name)
	}
	if err := validateResolvedResource(i, r); err != nil {
		return ResourceInfo{}, err
	}
	return ResourceInfo{
		Name:       r.Name,
		Kind:       r.Kind,
		APIVersion: r.APIVersion,
		Group:      r.Group,
		Version:    r.Version,
		Resource:   r.Resource,
		Scope:      r.Scope,
		Owner:      r.Owner,
	}, nil
}

func validateResolvedResource(i int, r WatchResource) error {
	if r.Version == "" || !versionRe.MatchString(r.Version) {
		return fmt.Errorf("watch.resources[%d].version: invalid version %q", i, r.Version)
	}
	if r.Group != "" && strings.ContainsAny(r.Group, " /") {
		return fmt.Errorf("watch.resources[%d].group: invalid group %q", i, r.Group)
	}
	if !resourceRe.MatchString(r.Resource) {
		return fmt.Errorf("watch.resources[%d].resource: invalid resource %q (use the lowercase plural resource name)", i, r.Resource)
	}
	if r.Scope != ScopeNamespaced && r.Scope != ScopeCluster {
		return fmt.Errorf("watch.resources[%d].scope: must be %q or %q", i, ScopeNamespaced, ScopeCluster)
	}
	if r.Scope == ScopeCluster && len(r.Namespaces) > 0 {
		return fmt.Errorf("watch.resources[%d]: scope=%q cannot set namespaces", i, ScopeCluster)
	}
	if legacy, ok := SupportedResource(r.Kind); ok && legacy.GVR() == (schema.GroupVersionResource{Group: r.Group, Version: r.Version, Resource: r.Resource}) {
		if legacy.Scope == ScopeCluster && r.Scope != ScopeCluster {
			return fmt.Errorf("watch.resources[%d]: kind %q must use scope=%q", i, r.Kind, ScopeCluster)
		}
	}
	return nil
}

func groupVersionForResource(r WatchResource) (schema.GroupVersion, error) {
	var gv schema.GroupVersion
	if r.APIVersion != "" {
		parsed, err := schema.ParseGroupVersion(r.APIVersion)
		if err != nil {
			return schema.GroupVersion{}, fmt.Errorf("apiVersion %q is invalid: %w", r.APIVersion, err)
		}
		gv = parsed
	}
	if r.Group != "" || r.Version != "" {
		if r.Version == "" {
			return schema.GroupVersion{}, fmt.Errorf("version: required when group is set")
		}
		if gv.Version != "" && (gv.Group != r.Group || gv.Version != r.Version) {
			return schema.GroupVersion{}, fmt.Errorf("apiVersion %q conflicts with group/version %q/%q", r.APIVersion, r.Group, r.Version)
		}
		gv = schema.GroupVersion{Group: r.Group, Version: r.Version}
	}
	if gv.Version == "" {
		return schema.GroupVersion{}, fmt.Errorf("apiVersion or version: required")
	}
	return gv, nil
}

func applyLegacyDefaults(r WatchResource) WatchResource {
	if r.Name == "" {
		r.Name = r.Kind
	}
	info, ok := SupportedResource(r.Kind)
	if !ok || r.Resource != "" || r.APIVersion != "" || r.Group != "" || r.Version != "" {
		return r
	}
	r.APIVersion = info.GroupVersion()
	r.Group = info.Group
	r.Version = info.Version
	r.Resource = info.Resource
	if r.Scope == "" {
		r.Scope = info.Scope
	}
	if r.Owner.Follow == nil && !r.Owner.TopController {
		r.Owner = info.Owner
	}
	return r
}

func isLegacyKindOnly(r WatchResource) bool {
	_, ok := SupportedResource(r.Kind)
	return ok && r.APIVersion == "" && r.Group == "" && r.Version == "" && r.Resource == ""
}

func isCompleteResource(r WatchResource) bool {
	r = applyLegacyDefaults(r)
	return r.Kind != "" && r.Version != "" && r.Resource != "" && r.Scope != ""
}

func resourceName(r WatchResource) string {
	if r.Name != "" {
		return r.Name
	}
	return r.Kind
}

// chIdentRe matches a legal ClickHouse column/table identifier.
var chIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// historyColumnTypes is the allowlist of ClickHouse column types a history
// column may declare. The type is authoritative: the exporter marshals the
// json-path value into this type and generates matching DDL.
var historyColumnTypes = map[string]struct{}{
	"String": {}, "Array(String)": {}, "Int64": {}, "UInt64": {},
	"Float64": {}, "Bool": {}, "DateTime64(3)": {},
}

// historyFilterOps is the allowlist of filter operators. Ordering guarantees
// (cheap ops before regex, short-circuit) are enforced at evaluation time.
var historyFilterOps = map[string]struct{}{
	"exists": {}, "equals": {}, "notEquals": {}, "prefix": {},
	"suffix": {}, "contains": {}, "in": {}, "regex": {},
}

// reservedHistoryColumns are the implicit envelope columns present on every
// history table; a resource may not redeclare them.
var reservedHistoryColumns = map[string]struct{}{
	"namespace": {}, "name": {}, "uid": {}, "resource_version": {},
	"valid_from": {}, "valid_to": {}, "deleted": {}, "spec_hash": {}, "ingest_seq": {},
}

// Batch defaults for the history ingest writer.
const (
	DefaultBatchMaxRows = 5000
	DefaultBatchFlushMs = 1000
)

// History configures the optional event-driven ClickHouse version store.
// Every informer event for a declared resource appends a version row; the store
// is append-only, and valid_to is materialized by re-inserting the superseded
// row with its end time (ReplacingMergeTree collapses the pair).
type History struct {
	Enabled   bool              `json:"enabled,omitempty"`
	Store     StoreConfig       `json:"store,omitempty"`
	Resources []HistoryResource `json:"resources,omitempty"`
}

// StoreConfig describes the ClickHouse backend for the history store.
type StoreConfig struct {
	// Type currently must be "clickhouse".
	Type string `json:"type,omitempty"`
	// DSN is a ClickHouse connection string. Supported schemes:
	//   clickhouse://host:9000/db  — native TCP protocol (default / recommended)
	//   http://host:8123/db        — HTTP interface (e.g. via ingress / proxy)
	//   https://host/db            — HTTP over TLS; pair with secure: true
	// clickhouse://host:8123 still speaks native and will fail against an HTTP
	// listener — the scheme, not the port, selects the protocol.
	DSN string `json:"dsn,omitempty"`
	// CreateSchema toggles auto-DDL. nil/false (prod-safe default) means the
	// exporter only validates the live schema against config and fails fast on
	// drift, mutating nothing. true (dev) means CREATE TABLE IF NOT EXISTS plus
	// additive ADD COLUMN IF NOT EXISTS. It never drops or retypes columns.
	CreateSchema *bool `json:"createSchema,omitempty"`
	// CloseMode selects how a superseded/deleted version's valid_to is
	// materialized. "rewrite" (default) re-inserts the prior row with valid_to
	// filled in and a higher ingest_seq, collapsed later by
	// ReplacingMergeTree — works on any ClickHouse version but leaves
	// transient duplicate rows until background merges run. "update" closes
	// the open row in place with a lightweight UPDATE (patch parts) so every
	// version is always exactly one row — requires ClickHouse >= 25.8 and the
	// enable_block_number_column / enable_block_offset_column table settings
	// (added automatically when createSchema is on). See
	// docs/lightweight-update-upgrade-plan.md.
	CloseMode string `json:"closeMode,omitempty"`
	// Batch tunes the ingest writer.
	Batch BatchConfig `json:"batch,omitempty"`

	// --- Authentication (all optional). When set, these override any
	// credentials embedded in DSN. For secrets, prefer the *Env variants
	// (populated from a Kubernetes Secret) so no plaintext lives in the
	// ConfigMap; the *Env form takes precedence over its inline counterpart.
	// token/tokenEnv (JWT / ClickHouse Cloud access token) is mutually
	// exclusive with username/password (it replaces basic auth). ---

	// Username is the ClickHouse user (inline).
	Username string `json:"username,omitempty"`
	// UsernameEnv names an env var holding the username; takes precedence.
	UsernameEnv string `json:"usernameEnv,omitempty"`
	// Password is the ClickHouse password (inline; dev only).
	Password string `json:"password,omitempty"`
	// PasswordEnv names an env var holding the password; takes precedence.
	PasswordEnv string `json:"passwordEnv,omitempty"`
	// Database overrides the target database.
	Database string `json:"database,omitempty"`
	// Token is a JWT / access token (inline). Replaces username/password.
	Token string `json:"token,omitempty"`
	// TokenEnv names an env var holding the token; takes precedence.
	TokenEnv string `json:"tokenEnv,omitempty"`
	// Secure enables TLS to ClickHouse (required by ClickHouse Cloud).
	Secure *bool `json:"secure,omitempty"`
	// TLSSkipVerify disables server certificate verification (test/self-signed).
	TLSSkipVerify bool `json:"tlsSkipVerify,omitempty"`
}

// CreateSchemaEnabled reports whether auto-DDL is turned on.
func (s StoreConfig) CreateSchemaEnabled() bool {
	return s.CreateSchema != nil && *s.CreateSchema
}

// Close-mode values for StoreConfig.CloseMode.
const (
	CloseModeRewrite = "rewrite"
	CloseModeUpdate  = "update"
)

// CloseModeOrDefault returns the configured close mode, defaulting to rewrite
// (the mode with no ClickHouse version requirement).
func (s StoreConfig) CloseModeOrDefault() string {
	if s.CloseMode == "" {
		return CloseModeRewrite
	}
	return s.CloseMode
}

// SecureEnabled reports whether TLS is turned on.
func (s StoreConfig) SecureEnabled() bool {
	return s.Secure != nil && *s.Secure
}

// resolveCred returns the value of envName (if set) or the inline fallback.
// When envName is set but the environment variable is empty, it is treated as
// a misconfiguration and an error is returned.
func resolveCred(kind, envName, inline string) (string, error) {
	if envName == "" {
		return inline, nil
	}
	v := os.Getenv(envName)
	if v == "" {
		return "", fmt.Errorf("store.%sEnv references empty environment variable %q", kind, envName)
	}
	return v, nil
}

// ResolveUsername returns the username from UsernameEnv (preferred) or Username.
func (s StoreConfig) ResolveUsername() (string, error) {
	return resolveCred("username", s.UsernameEnv, s.Username)
}

// ResolvePassword returns the password from PasswordEnv (preferred) or Password.
func (s StoreConfig) ResolvePassword() (string, error) {
	return resolveCred("password", s.PasswordEnv, s.Password)
}

// ResolveToken returns the token from TokenEnv (preferred) or Token.
func (s StoreConfig) ResolveToken() (string, error) {
	return resolveCred("token", s.TokenEnv, s.Token)
}

// BatchConfig tunes the ingest batch writer.
type BatchConfig struct {
	MaxRows         int `json:"maxRows,omitempty"`
	FlushIntervalMs int `json:"flushIntervalMs,omitempty"`
}

// MaxRowsOrDefault returns the configured batch size or the default.
func (b BatchConfig) MaxRowsOrDefault() int {
	if b.MaxRows > 0 {
		return b.MaxRows
	}
	return DefaultBatchMaxRows
}

// FlushIntervalMsOrDefault returns the configured flush interval or the default.
func (b BatchConfig) FlushIntervalMsOrDefault() int {
	if b.FlushIntervalMs > 0 {
		return b.FlushIntervalMs
	}
	return DefaultBatchFlushMs
}

// HistoryResource declares one watched Kind's columns and filters. The Kind
// must also appear in watch.resources so the informer cache is populated.
type HistoryResource struct {
	Kind    string          `json:"kind"`
	Table   string          `json:"table,omitempty"`
	Columns []HistoryColumn `json:"columns,omitempty"`
	Filters []HistoryFilter `json:"filters,omitempty"`
}

// TableName returns the configured table or a default derived from the Kind.
func (r HistoryResource) TableName() string {
	if r.Table != "" {
		return r.Table
	}
	return strings.ToLower(r.Kind) + "_versions"
}

// HistoryColumn declares one ClickHouse column and how to populate it. The
// embedded Extract supplies Source/Path/Fallbacks/OnMissing (json path
// extraction), reusing the same engine as metric labels.
type HistoryColumn struct {
	Extract
	// Name is the ClickHouse column name.
	Name string `json:"name"`
	// Type is one of the allowlisted ClickHouse types.
	Type string `json:"type"`
	// Encode transforms the extracted value: "" (scalar), "json" (marshal the
	// extracted subtree to a JSON string; requires type String), "kv" (flatten
	// a map into sorted "k=v" tokens; requires type Array(String)).
	Encode string `json:"encode,omitempty"`
	// Index optionally attaches a skip index: "" or "bloom_filter".
	Index string `json:"index,omitempty"`
}

// HistoryFilter declares one client-side predicate on an extracted field. All
// filters on a resource must pass (AND) for the object to be written.
type HistoryFilter struct {
	Extract
	// Op is one of the allowlisted operators.
	Op string `json:"op"`
	// Value is the operand for scalar ops (equals/prefix/suffix/contains/regex).
	Value string `json:"value,omitempty"`
	// Values is the operand set for op=in.
	Values []string `json:"values,omitempty"`
	// Negate inverts the match result.
	Negate bool `json:"negate,omitempty"`
}

func (h History) validate(reg *Registry) error {
	if h.Store.Type != "clickhouse" {
		return fmt.Errorf("store.type: only \"clickhouse\" is supported (got %q)", h.Store.Type)
	}
	if strings.TrimSpace(h.Store.DSN) == "" {
		return fmt.Errorf("store.dsn: required")
	}
	switch h.Store.CloseMode {
	case "", CloseModeRewrite, CloseModeUpdate:
	default:
		return fmt.Errorf("store.closeMode: must be %q or %q (got %q)", CloseModeRewrite, CloseModeUpdate, h.Store.CloseMode)
	}
	// token/JWT replaces basic auth in clickhouse-go, so the two are mutually
	// exclusive.
	hasToken := h.Store.Token != "" || h.Store.TokenEnv != ""
	hasBasic := h.Store.Username != "" || h.Store.UsernameEnv != "" ||
		h.Store.Password != "" || h.Store.PasswordEnv != ""
	if hasToken && hasBasic {
		return fmt.Errorf("store: token/tokenEnv is mutually exclusive with username/password (JWT replaces basic auth)")
	}
	if len(h.Resources) == 0 {
		return fmt.Errorf("resources: at least one resource is required when history.enabled")
	}
	seenTable := map[string]struct{}{}
	for i := range h.Resources {
		r := &h.Resources[i]
		if err := r.validate(reg); err != nil {
			return fmt.Errorf("resources[%d] (kind=%q): %w", i, r.Kind, err)
		}
		table := r.TableName()
		if !chIdentRe.MatchString(table) {
			return fmt.Errorf("resources[%d]: table %q is not a valid ClickHouse identifier", i, table)
		}
		if _, dup := seenTable[table]; dup {
			return fmt.Errorf("resources[%d]: table %q is duplicated", i, table)
		}
		seenTable[table] = struct{}{}
	}
	return nil
}

func (r *HistoryResource) validate(reg *Registry) error {
	if strings.TrimSpace(r.Kind) == "" {
		return fmt.Errorf("kind: required")
	}
	if _, err := reg.ResourceFor(r.Kind); err != nil {
		return fmt.Errorf("kind %q is required by history but not included in watch.resources: %w", r.Kind, err)
	}
	if len(r.Columns) == 0 {
		return fmt.Errorf("columns: at least one column is required")
	}
	seen := map[string]struct{}{}
	for i := range r.Columns {
		c := &r.Columns[i]
		if err := c.validate(reg); err != nil {
			return fmt.Errorf("columns[%d] (name=%q): %w", i, c.Name, err)
		}
		if _, reserved := reservedHistoryColumns[c.Name]; reserved {
			return fmt.Errorf("columns[%d]: name %q is a reserved envelope column", i, c.Name)
		}
		if _, dup := seen[c.Name]; dup {
			return fmt.Errorf("columns[%d]: column %q is duplicated", i, c.Name)
		}
		seen[c.Name] = struct{}{}
	}
	for i := range r.Filters {
		if err := r.Filters[i].validate(reg); err != nil {
			return fmt.Errorf("filters[%d]: %w", i, err)
		}
	}
	return nil
}

func (c *HistoryColumn) validate(reg *Registry) error {
	if !chIdentRe.MatchString(c.Name) {
		return fmt.Errorf("name %q: invalid ClickHouse identifier", c.Name)
	}
	if _, ok := historyColumnTypes[c.Type]; !ok {
		return fmt.Errorf("type %q: unsupported (allowed: String, Array(String), Int64, UInt64, Float64, Bool, DateTime64(3))", c.Type)
	}
	switch c.Encode {
	case "":
	case "json":
		if c.Type != "String" {
			return fmt.Errorf("encode=json requires type String")
		}
	case "kv":
		if c.Type != "Array(String)" {
			return fmt.Errorf("encode=kv requires type Array(String)")
		}
	default:
		return fmt.Errorf("encode %q: unsupported (allowed: json, kv)", c.Encode)
	}
	switch c.Index {
	case "", "bloom_filter":
	default:
		return fmt.Errorf("index %q: unsupported (allowed: bloom_filter)", c.Index)
	}
	// Reuse Extract validation (history has no forEach context).
	return c.Extract.validate(reg, false)
}

func (f *HistoryFilter) validate(reg *Registry) error {
	if _, ok := historyFilterOps[f.Op]; !ok {
		return fmt.Errorf("op %q: unsupported (allowed: exists, equals, notEquals, prefix, suffix, contains, in, regex)", f.Op)
	}
	switch f.Op {
	case "exists":
		// no operand required
	case "in":
		if len(f.Values) == 0 {
			return fmt.Errorf("op=in requires values")
		}
	case "regex":
		if f.Value == "" {
			return fmt.Errorf("op=regex requires value")
		}
		if _, err := regexp.Compile(f.Value); err != nil {
			return fmt.Errorf("op=regex: invalid pattern %q: %w", f.Value, err)
		}
	default:
		if f.Value == "" {
			return fmt.Errorf("op=%s requires value", f.Op)
		}
	}
	return f.Extract.validate(reg, false)
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
	reg, err := c.buildRegistry(c.Discovery.Enabled)
	if err != nil {
		return err
	}
	seenMetric := map[string]struct{}{}
	for i := range c.Rules {
		r := &c.Rules[i]
		if err := r.validate(reg); err != nil {
			return fmt.Errorf("rules[%d] (name=%q): %w", i, r.Name, err)
		}
		if err := r.validateAgainstWatch(reg); err != nil {
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
	if c.History.Enabled {
		if err := c.History.validate(reg); err != nil {
			return fmt.Errorf("history: %w", err)
		}
	}
	return nil
}

// buildRegistry checks watch.resources schema and constructs the resource
// lookup tables used by validation, informers, and owner-chain resolution.
func (c *Config) buildRegistry(allowDiscoveryPlaceholders bool) (*Registry, error) {
	reg := &Registry{
		byName:      map[string]ResourceInfo{},
		byGVK:       map[schema.GroupVersionKind]ResourceInfo{},
		byGVR:       map[schema.GroupVersionResource]ResourceInfo{},
		kindToNames: map[string][]string{},
	}
	for i, raw := range c.Watch.Resources {
		info, err := normalizeResource(i, raw, allowDiscoveryPlaceholders)
		if err != nil {
			return nil, err
		}
		if _, dup := reg.byName[info.Name]; dup {
			return nil, fmt.Errorf("watch.resources[%d].name: duplicated resource name %q", i, info.Name)
		}
		if info.Resource != "" {
			if prev, dup := reg.byGVR[info.GVR()]; dup {
				return nil, fmt.Errorf("watch.resources[%d]: duplicated GVR %s (already used by %q)", i, info.GVR().String(), prev.Name)
			}
			reg.byGVR[info.GVR()] = info
		}
		if info.Version != "" && info.Kind != "" {
			if prev, dup := reg.byGVK[info.GVK()]; dup {
				return nil, fmt.Errorf("watch.resources[%d]: duplicated GVK %s (already used by %q)", i, info.GVK().String(), prev.Name)
			}
			reg.byGVK[info.GVK()] = info
		}
		reg.resources = append(reg.resources, info)
		reg.byName[info.Name] = info
		reg.kindToNames[info.Kind] = append(reg.kindToNames[info.Kind], info.Name)
	}
	return reg, nil
}

// requiredWatchResources are resources the rule's anchor and explicit
// label/expandLabel sources need to have in the informer cache (excludes
// ownerController/topController).
func (r *Rule) requiredWatchResources(reg *Registry) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	anchor, err := reg.ResourceFor(r.Anchor)
	if err != nil {
		return nil, fmt.Errorf("anchor: %w", err)
	}
	out[anchor.Name] = struct{}{}
	add := func(src string) error {
		switch src {
		case "", "anchor", "item", "ownerController", "topController":
			return nil
		}
		info, err := reg.ResourceFor(src)
		if err != nil {
			return err
		}
		out[info.Name] = struct{}{}
		return nil
	}
	for _, ext := range r.Labels {
		if err := add(ext.EffectiveSource()); err != nil {
			return nil, err
		}
		for _, f := range ext.Fallbacks {
			if err := add(f.EffectiveSource()); err != nil {
				return nil, err
			}
		}
	}
	for _, ex := range r.ExpandLabels {
		if err := add(ex.EffectiveSource()); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (r *Rule) validateAgainstWatch(reg *Registry) error {
	required, err := r.requiredWatchResources(reg)
	if err != nil {
		return err
	}
	for name := range required {
		if _, ok := reg.byName[name]; !ok {
			return fmt.Errorf("resource %q is required by anchor/sources but is not included in watch.resources", name)
		}
	}
	return nil
}

// validSourceSet returns the set of legal source names recognised by labels
// and expandLabels: built-ins plus every supported Kind name. Relation
// aliases are no longer supported; refer to topController / ownerController
// or a kind name directly.
func (r *Rule) validate(reg *Registry) error {
	if strings.TrimSpace(r.Name) == "" {
		return fmt.Errorf("name: required")
	}
	if _, err := reg.ResourceFor(r.Anchor); err != nil {
		return fmt.Errorf("anchor: %w", err)
	}
	if len(r.Labels) == 0 && len(r.ExpandLabels) == 0 {
		return fmt.Errorf("labels: at least one fixed label or expandLabels entry is required")
	}

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
		if err := extract.validate(reg, hasForEach); err != nil {
			return fmt.Errorf("labels[%q]: %w", name, err)
		}
		fixedNames[name] = struct{}{}
	}

	for i, ex := range r.ExpandLabels {
		if err := ex.validate(reg, hasForEach); err != nil {
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

func (e Extract) validate(reg *Registry, forEachActive bool) error {
	src := e.EffectiveSource()
	if err := validateSource(reg, src, forEachActive); err != nil {
		return err
	}
	if strings.TrimSpace(e.Path) == "" {
		return fmt.Errorf("path: required")
	}
	for i, f := range e.Fallbacks {
		if len(f.Fallbacks) > 0 {
			return fmt.Errorf("fallbacks[%d]: nested fallbacks are not supported", i)
		}
		if err := f.validate(reg, forEachActive); err != nil {
			return fmt.Errorf("fallbacks[%d]: %w", i, err)
		}
	}
	return nil
}

func (e ExpandLabel) validate(reg *Registry, forEachActive bool) error {
	src := e.EffectiveSource()
	if err := validateSource(reg, src, forEachActive); err != nil {
		return err
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

func validateSource(reg *Registry, src string, forEachActive bool) error {
	switch src {
	case "", "anchor", "ownerController", "topController":
		return nil
	case "item":
		if !forEachActive {
			return fmt.Errorf("source=item requires forEach on the rule")
		}
		return nil
	default:
		if _, err := reg.ResourceFor(src); err != nil {
			return fmt.Errorf("source %q is not recognised: %w", src, err)
		}
		return nil
	}
}
