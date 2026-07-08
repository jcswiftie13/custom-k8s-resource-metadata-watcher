//go:build integration

package e2e

import (
	"fmt"
	"strings"
)

// istioHistoryConfigYAML renders an exporter config that mirrors the POC's
// ClickHouse routing-history shape: it watches the POC resource set (Service,
// Deployment, Istio Gateway, VirtualService) and materialises four version
// tables (service_versions / deploy_versions / gw_versions / vs_versions) with
// the same domain/join columns and bloom indexes as poc/route2a/internal/chstore
// — reproduced purely through history config, with no import of poc code.
//
// It exercises all four required aspects in one scenario:
//   - create table: createSchema:true builds the four tables + bloom indexes.
//   - type: String / Int64 / Array(String) / encode:kv / encode:json columns.
//   - filter: the Service history resource keeps only type in (LoadBalancer,NodePort).
//   - labelSelector: the Gateway watch is narrowed server-side to istio=ingressgateway.
func istioHistoryConfigYAML(namespace string) string {
	return fmt.Sprintf(`metricPrefix: "it_"

watch:
  resources:
    - kind: Service
      scope: Namespaced
      namespaces:
        - %[1]s
    - kind: Deployment
      scope: Namespaced
      namespaces:
        - %[1]s
    - name: Gateway
      group: networking.istio.io
      version: v1beta1
      resource: gateways
      kind: Gateway
      scope: Namespaced
      namespaces:
        - %[1]s
      labelSelector: "istio=ingressgateway"
    - name: VirtualService
      group: networking.istio.io
      version: v1beta1
      resource: virtualservices
      kind: VirtualService
      scope: Namespaced
      namespaces:
        - %[1]s

rules:
  - name: "service_info"
    help: "Trivial rule so the config validates; history is the real subject."
    anchor: Service
    labels:
      namespace:
        path: "metadata.namespace"
      service:
        path: "metadata.name"

history:
  enabled: true
  store:
    type: clickhouse
    dsn: "clickhouse://default@clickhouse:9000/default"
    createSchema: true
    batch:
      maxRows: 100
      flushIntervalMs: 200
  resources:
    - kind: Service
      table: service_versions
      columns:
        - { name: ingress_ips, type: "Array(String)", path: "status.loadBalancer.ingress[*].ip", index: bloom_filter }
        - { name: selector_kv, type: "Array(String)", path: "spec.selector", encode: kv }
        - { name: port, type: "Int64", path: "spec.ports[0].port" }
        - { name: port_name, type: "String", path: "spec.ports[0].name" }
        - { name: protocol, type: "String", path: "spec.ports[0].protocol" }
        - { name: hostname, type: "String", path: "metadata.name" }
      filters:
        - { path: "spec.type", op: in, values: ["LoadBalancer", "NodePort"] }
    - kind: Deployment
      table: deploy_versions
      columns:
        - { name: pod_labels_kv, type: "Array(String)", path: "spec.template.metadata.labels", encode: kv }
    - kind: Gateway
      table: gw_versions
      columns:
        - { name: selector_kv, type: "Array(String)", path: "spec.selector", encode: kv, index: bloom_filter }
        - { name: server_hosts, type: "Array(String)", path: "spec.servers[*].hosts[*]" }
        - { name: spec_json, type: "String", path: "spec", encode: json }
    - kind: VirtualService
      table: vs_versions
      columns:
        - { name: bound_gateways, type: "Array(String)", path: "spec.gateways[*]" }
        - { name: spec_json, type: "String", path: "spec", encode: json }
`, namespace)
}

// sharedRulesYAML is the integration test rule set for cluster-wide and
// per-namespace topologies. The "controller_*" labels exercise the
// owner-chain (Pod -> ReplicaSet -> Deployment via topController) under the
// new custom-collector / scrape-time architecture.
const sharedRulesYAML = `
rules:
  - name: "pod_info"
    help: "Integration test: one series per Pod."
    anchor: Pod
    labels:
      namespace:
        path: "metadata.namespace"
      pod:
        path: "metadata.name"
      phase:
        path: "status.phase"
      controller_kind:
        source: topController
        path: "kind"
      controller_name:
        source: topController
        path: "metadata.name"
      controller_annotation_integration_test_controller_note:
        source: topController
        path: 'metadata.annotations["integration.test/controller-note"]'

  - name: "pod_container_info"
    help: "Integration test: one series per (Pod, container)."
    anchor: Pod
    forEach: "spec.containers[*]"
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
        source: topController
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

  - name: "service_info"
    help: "Integration test: one series per Service."
    anchor: Service
    labels:
      namespace:
        path: "metadata.namespace"
      service:
        path: "metadata.name"
      type:
        path: "spec.type"
      cluster_ip:
        path: "spec.clusterIP"

  - name: "endpointslice_info"
    help: "Integration test: one series per EndpointSlice."
    anchor: EndpointSlice
    labels:
      namespace:
        path: "metadata.namespace"
      endpointslice:
        path: "metadata.name"
      service:
        source: Service
        path: "metadata.name"
      addr_type:
        path: "addressType"
`

// podRulesYAML contains only pod-anchored rules for subset watch tests where
// Node is intentionally excluded from watch.resources.
const podRulesYAML = `
rules:
  - name: "pod_info"
    help: "Integration test: one series per Pod."
    anchor: Pod
    labels:
      namespace:
        path: "metadata.namespace"
      pod:
        path: "metadata.name"
      phase:
        path: "status.phase"
      controller_kind:
        source: topController
        path: "kind"
      controller_name:
        source: topController
        path: "metadata.name"
      controller_annotation_integration_test_controller_note:
        source: topController
        path: 'metadata.annotations["integration.test/controller-note"]'

  - name: "pod_container_info"
    help: "Integration test: one series per (Pod, container)."
    anchor: Pod
    forEach: "spec.containers[*]"
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
        source: topController
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

// dynamicMetadataConfigYAML is a minimal config dedicated to dynamic
// expandLabels coverage in e2e tests. It keeps one Pod rule and one Node rule
// with fixed identity labels plus dynamic labels/annotations flattening.
func dynamicMetadataConfigYAML() string {
	return `metricPrefix: "it_"

watch:
  resources:
    - kind: Pod
      scope: Namespaced
      fieldSelector: "status.phase!=Succeeded"
    - kind: Node
      scope: Cluster

rules:
  - name: "pod_dynamic_metadata"
    help: "Integration test: dynamic pod metadata labels."
    anchor: Pod
    labels:
      namespace:
        path: "metadata.namespace"
      pod:
        path: "metadata.name"
    expandLabels:
      - source: anchor
        path: "metadata.labels"
        prefix: "label_"
      - source: anchor
        path: "metadata.annotations"
        prefix: "annotation_"

  - name: "node_dynamic_metadata"
    help: "Integration test: dynamic node metadata labels."
    anchor: Node
    labels:
      node_name:
        path: "metadata.name"
    expandLabels:
      - source: anchor
        path: "metadata.labels"
        prefix: "label_"
      - source: anchor
        path: "metadata.annotations"
        prefix: "annotation_"
`
}

func customGVRConfigYAML(namespace, group, version, resource, kind string) string {
	return `metricPrefix: "it_"

watch:
  resources:
    - name: ` + kind + `
      group: ` + group + `
      version: ` + version + `
      resource: ` + resource + `
      kind: ` + kind + `
      scope: Namespaced
      namespaces:
        - ` + namespace + `

rules:
  - name: "widget_info"
    help: "Integration test: one series per custom Widget."
    anchor: ` + kind + `
    labels:
      namespace:
        path: "metadata.namespace"
      widget:
        path: "metadata.name"
      size:
        path: "spec.size"
`
}

func customGVRDiscoveryConfigYAML(namespace, apiVersion, kind string) string {
	return `metricPrefix: "it_"

discovery:
  enabled: true

watch:
  resources:
    - name: ` + kind + `
      apiVersion: ` + apiVersion + `
      kind: ` + kind + `
      namespaces:
        - ` + namespace + `

rules:
  - name: "widget_info"
    help: "Integration test: one series per custom Widget."
    anchor: ` + kind + `
    labels:
      namespace:
        path: "metadata.namespace"
      widget:
        path: "metadata.name"
      size:
        path: "spec.size"
`
}

// renderConfigYAML emits the full exporter config body with an explicit
// watch.resources set. When namespaces is nil/empty, omitted namespaces mean
// each namespaced resource is watched cluster-wide.
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
    - kind: Service
      scope: Namespaced
` + podNsBlock + `    - kind: EndpointSlice
      scope: Namespaced
` + podNsBlock + sharedRulesYAML
}
