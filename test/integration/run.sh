#!/usr/bin/env bash
# Kind integration runner: provisions a Kind cluster, builds + loads the
# exporter image, applies the kustomize base, waits for the exporter to be
# ready, then hands off to `go test -tags integration` which owns all of
# the assertions (see docs/INTEGRATION_TESTS.md for scenarios).
#
# Environment variables:
#   KIND_CLUSTER_NAME   kind cluster name (default metadata-exporter-it)
#   INTEGRATION_IMAGE   image tag to build + load (default metadata-exporter:it)
#   SKIP_KIND_CREATE    when set, use the current kubeconfig context and
#                       neither create nor delete a Kind cluster
#                       When unset, an existing cluster with the same name is
#                       reused if its node count matches kind-config.yaml;
#                       otherwise it is deleted and recreated.
#   SKIP_CLUSTER_DELETE when set together with create, keep the cluster
#                       after the run (useful for debugging)
#   SKIP_GO_TEST        when set, apply manifests but do not invoke go test
#   DOCKER_BUILD_PLATFORM  optional --platform for `docker build`
#   GOTEST_FLAGS        additional flags passed to `go test`
#   INTEGRATION_PATCH_NODE_EXTERNAL_IP  set to 0 to skip RFC5737 ExternalIP
#                       patches on Node status (default: patch when not 0)
#   INTEGRATION_PRINT_METRICS  set to 1 to print per-test allowlisted exporter
#                       /metrics sample lines at the end of each metric-related
#                       Go test via t.Log; unset prints nothing (handled by Go tests)
#   INTEGRATION_PORT_FORWARD_METRICS  set to 1 to run kubectl port-forward to
#                       svc/metadata-exporter after tests; requires the cluster
#                       to be kept (SKIP_CLUSTER_DELETE=1 if this runner
#                       created the cluster, or SKIP_KIND_CREATE with your own)
#   INTEGRATION_METRICS_LOCAL_PORT  local port for port-forward (default 18080)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${ROOT}"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-metadata-exporter-it}"
IMAGE="${INTEGRATION_IMAGE:-metadata-exporter:it}"
NS="metadata-exporter-it"

log() { printf '%s\n' "$*"; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    log "error: required command not found: $1"
    exit 1
  }
}

need_cmd kind
need_cmd kubectl
need_cmd docker
need_cmd go

CREATED_CLUSTER=0
KUBECONFIG_IT_FILE=""

cleanup() {
  local exit_code=$?
  if [[ "${CREATED_CLUSTER}" -eq 1 ]] && [[ -z "${SKIP_CLUSTER_DELETE:-}" ]]; then
    if [[ -n "${KUBECONFIG_IT_FILE}" ]] && [[ -f "${KUBECONFIG_IT_FILE}" ]]; then
      KUBECONFIG="${KUBECONFIG_IT_FILE}" kind delete cluster --name "${KIND_CLUSTER_NAME}" || true
    else
      kind delete cluster --name "${KIND_CLUSTER_NAME}" || true
    fi
  fi
  if [[ -n "${KUBECONFIG_IT_FILE}" ]]; then
    rm -f "${KUBECONFIG_IT_FILE}"
  fi
  exit "${exit_code}"
}
trap cleanup EXIT

# shellcheck source=kind_ensure_cluster.inc.sh
source "${SCRIPT_DIR}/kind_ensure_cluster.inc.sh"

if [[ -n "${SKIP_KIND_CREATE:-}" ]]; then
  log "SKIP_KIND_CREATE set: using current kube context (cluster will not be deleted)"
else
  kind_integration_ensure_cluster || exit 1
fi

if [[ "${INTEGRATION_PORT_FORWARD_METRICS:-0}" == "1" ]]; then
  if [[ "${CREATED_CLUSTER}" -eq 1 ]] && [[ -z "${SKIP_CLUSTER_DELETE:-}" ]]; then
    log "error: INTEGRATION_PORT_FORWARD_METRICS=1 requires SKIP_CLUSTER_DELETE=1 when this script creates the Kind cluster"
    exit 1
  fi
fi

log "waiting for all nodes to be Ready"
kubectl wait --for=condition=Ready nodes --all --timeout=5m

bash "${SCRIPT_DIR}/patch_kind_node_external_ips.sh"

DOCKER_BUILD_PLATFORM="${DOCKER_BUILD_PLATFORM:-}"
build_args=(build -t "${IMAGE}" .)
if [[ -n "${DOCKER_BUILD_PLATFORM}" ]]; then
  build_args=(build --platform "${DOCKER_BUILD_PLATFORM}" -t "${IMAGE}" .)
fi
log "docker ${build_args[*]}"
docker "${build_args[@]}"

kind load docker-image "${IMAGE}" --name "${KIND_CLUSTER_NAME}"

kubectl apply -k "${SCRIPT_DIR}/manifests"

kubectl rollout status deployment/metadata-exporter -n "${NS}" --timeout=120s
kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/name=metadata-exporter-integration \
  -n "${NS}" --timeout=120s

run_go_tests() {
  export E2E_REPO_ROOT="${ROOT}"
  # shellcheck disable=SC2086
  go test -tags integration -v -count=1 -timeout=20m ${GOTEST_FLAGS:-} ./test/integration/e2e/...
}

maybe_port_forward_metrics() {
  local tests_exit="$1"
  [[ "${INTEGRATION_PORT_FORWARD_METRICS:-0}" != "1" ]] && return 0
  local port="${INTEGRATION_METRICS_LOCAL_PORT:-18080}"
  log "starting kubectl port-forward (Ctrl+C to stop)"
  log "  URL: http://127.0.0.1:${port}/metrics"
  # Drop EXIT cleanup so SIGINT does not delete a cluster we intend to keep.
  trap - EXIT
  cleanup_pf() {
    if [[ -n "${KUBECONFIG_IT_FILE}" ]] && [[ -f "${KUBECONFIG_IT_FILE}" ]]; then
      rm -f "${KUBECONFIG_IT_FILE}" || true
    fi
    exit "${tests_exit}"
  }
  trap cleanup_pf INT TERM
  kubectl port-forward -n "${NS}" "svc/metadata-exporter" "${port}:8080" || log "port-forward exited (status $?)"
  cleanup_pf
}

if [[ -n "${SKIP_GO_TEST:-}" ]]; then
  log "SKIP_GO_TEST set: manifests applied; skipping go test"
  maybe_port_forward_metrics 0
  exit 0
fi

log "running integration go tests"
go_test_rc=0
run_go_tests || go_test_rc=$?

maybe_port_forward_metrics "${go_test_rc}"

exit "${go_test_rc}"
