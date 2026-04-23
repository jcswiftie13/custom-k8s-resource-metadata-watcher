//go:build integration

package e2e

import (
	"strings"
)

// clusterWideConfigYAML returns an exporter config that watches every
// namespace. The rule set is kept identical to the per-namespace variant so
// tests can compare metric shapes without caring about which topology is
// active.
func clusterWideConfigYAML() string {
	return renderConfigYAML(nil)
}

// perNamespaceConfigYAML returns an exporter config that only watches the
// supplied namespaces.
func perNamespaceConfigYAML(namespaces []string) string {
	return renderConfigYAML(namespaces)
}

// renderConfigYAML emits the full exporter config body. When namespaces is
// nil/empty the watch block uses `namespaces: []` which the collector
// interprets as cluster-wide.
func renderConfigYAML(namespaces []string) string {
	var nsBlock string
	if len(namespaces) == 0 {
		nsBlock = "  namespaces: []"
	} else {
		sb := &strings.Builder{}
		sb.WriteString("  namespaces:\n")
		for _, ns := range namespaces {
			sb.WriteString("    - ")
			sb.WriteString(ns)
			sb.WriteByte('\n')
		}
		nsBlock = strings.TrimRight(sb.String(), "\n")
	}
	return `metricPrefix: "it_"

watch:
` + nsBlock + `
  selectors:
    Pod:
      fieldSelector: "status.phase!=Succeeded"

rules:
  - name: "pod_info"
    help: "Integration test: one series per Pod."
    anchor: Pod
    relations:
      - name: top
        via: topController
    labels:
      namespace:
        path: "metadata.namespace"
      pod:
        path: "metadata.name"
      phase:
        path: "status.phase"
      controller_kind:
        source: top
        path: "kind"
      controller_name:
        source: top
        path: "metadata.name"
    flatten:
      - namePrefix: "controller_annotation_"
        source: top
        path: "metadata.annotations"
        keys:
          - "integration.test/controller-note"

  - name: "pod_container_info"
    help: "Integration test: one series per (Pod, container)."
    anchor: Pod
    forEach: "spec.containers[*]"
    relations:
      - name: top
        via: topController
    labels:
      namespace:
        path: "metadata.namespace"
      pod:
        path: "metadata.name"
      container:
        source: item
        path: "name"
      image:
        source: item
        path: "image"
      controller_name:
        source: top
        path: "metadata.name"
`
}
