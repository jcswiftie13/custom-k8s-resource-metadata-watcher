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
#   SKIP_CLUSTER_DELETE when set together with create, keep the cluster
#                       after the run (useful for debugging)
#   SKIP_GO_TEST        when set, apply manifests but do not invoke go test
#   DOCKER_BUILD_PLATFORM  optional --platform for `docker build`
#   GOTEST_FLAGS        additional flags passed to `go test`
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

if [[ -z "${SKIP_KIND_CREATE:-}" ]]; then
  if kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
    log "error: kind cluster ${KIND_CLUSTER_NAME} already exists (delete it or set SKIP_KIND_CREATE=1 to use current kubeconfig)"
    exit 1
  fi
  kind create cluster --name "${KIND_CLUSTER_NAME}" --wait 5m
  CREATED_CLUSTER=1
else
  log "SKIP_KIND_CREATE set: using current kube context (cluster will not be deleted)"
fi

if [[ "${CREATED_CLUSTER}" -eq 1 ]]; then
  KUBECONFIG_IT_FILE="$(mktemp)"
  kind get kubeconfig --name "${KIND_CLUSTER_NAME}" >"${KUBECONFIG_IT_FILE}"
  export KUBECONFIG="${KUBECONFIG_IT_FILE}"
fi

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

if [[ -n "${SKIP_GO_TEST:-}" ]]; then
  log "SKIP_GO_TEST set: manifests applied; exiting without running go test"
  exit 0
fi

log "running integration go tests"
export E2E_REPO_ROOT="${ROOT}"
# shellcheck disable=SC2086
go test -tags integration -v -count=1 -timeout=20m ${GOTEST_FLAGS:-} ./test/integration/e2e/...
