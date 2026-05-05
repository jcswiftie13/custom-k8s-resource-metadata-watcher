//go:build integration

package e2e

import (
	"strings"
)

// sharedRulesYAML is the integration test rule set for cluster-wide and
// per-namespace topologies.
const sharedRulesYAML = `
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
      controller_annotation_integration_test_controller_note:
        source: top
        path: 'metadata.annotations["integration.test/controller-note"]'

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

  - name: "node_info"
    help: "Integration test: one series per Node."
    anchor: Node
    labels:
      node_name:
        path: "metadata.name"
      system_uuid:
        path: "status.nodeInfo.systemUUID"

  - name: "node_address"
    help: "Integration test: one series per (Node, address)."
    anchor: Node
    forEach: "status.addresses[*]"
    labels:
      node_name:
        path: "metadata.name"
      address_type:
        source: item
        path: "type"
      address:
        source: item
        path: "address"

  - name: "node_condition"
    help: "Integration test: one series per (Node, condition)."
    anchor: Node
    forEach: "status.conditions[*]"
    labels:
      node_name:
        path: "metadata.name"
      condition:
        source: item
        path: "type"
      status:
        source: item
        path: "status"
`

// podRulesYAML contains only pod-anchored rules for subset watch tests where
// Node is intentionally excluded from watch.resources.
const podRulesYAML = `
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
      controller_annotation_integration_test_controller_note:
        source: top
        path: 'metadata.annotations["integration.test/controller-note"]'

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

// kindSubsetClusterWideConfigYAML is cluster-wide with only Pod and
// Deployment informers, for topology delta assertions.
func kindSubsetClusterWideConfigYAML() string {
	return `metricPrefix: "it_"

watch:
  resources:
    - kind: Pod
      scope: Cluster
    - kind: Deployment
      scope: Cluster
` + podRulesYAML
}

// renderConfigYAML emits the full exporter config body. When namespaces is
// nil/empty the watch block uses `namespaces: []` which the collector
// interprets as cluster-wide.
func renderConfigYAML(namespaces []string) string {
	var podNsBlock string
	if len(namespaces) > 0 {
		sb := &strings.Builder{}
		sb.WriteString("      namespaces:\n")
		for _, ns := range namespaces {
			sb.WriteString("        - ")
			sb.WriteString(ns)
			sb.WriteByte('\n')
		}
		podNsBlock = sb.String()
	}
	return `metricPrefix: "it_"

watch:
  resources:
    - kind: Pod
      scope: Namespaced
` + podNsBlock + `      fieldSelector: "status.phase!=Succeeded"
    - kind: ReplicaSet
      scope: Namespaced
` + podNsBlock + `    - kind: Deployment
      scope: Namespaced
` + podNsBlock + `    - kind: StatefulSet
      scope: Namespaced
` + podNsBlock + `    - kind: DaemonSet
      scope: Namespaced
` + podNsBlock + `    - kind: Node
      scope: Cluster
` + sharedRulesYAML
}
