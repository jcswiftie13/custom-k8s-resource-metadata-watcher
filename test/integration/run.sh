#!/usr/bin/env bash
# Kind integration test: build image, load into kind, apply manifests, assert /metrics.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${ROOT}"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-metadata-exporter-it}"
IMAGE="${INTEGRATION_IMAGE:-metadata-exporter:it}"
NS="metadata-exporter-it"
PF_LOCAL_PORT="${PF_LOCAL_PORT:-18080}"
METRICS_WAIT_SEC="${METRICS_WAIT_SEC:-90}"
DELETE_WAIT_SEC="${DELETE_WAIT_SEC:-60}"

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
need_cmd curl

CREATED_CLUSTER=0
PF_PID=""
# `kind get kubeconfig` writes YAML to stdout; KUBECONFIG must be a file path.
KUBECONFIG_IT_FILE=""

cleanup() {
  local exit_code=$?
  if [[ -n "${PF_PID}" ]] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
    wait "${PF_PID}" 2>/dev/null || true
  fi
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
kubectl rollout status deployment/fixture-web -n "${NS}" --timeout=120s
kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/name=metadata-exporter-integration \
  -n "${NS}" --timeout=120s

kubectl port-forward -n "${NS}" "svc/metadata-exporter" "${PF_LOCAL_PORT}:8080" &
PF_PID=$!

wait_http_ok() {
  local url=$1
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if curl -sf --max-time 2 "${url}" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

BASE_URL="http://127.0.0.1:${PF_LOCAL_PORT}"
wait_http_ok "${BASE_URL}/healthz" || {
  log "error: /healthz did not become ready"
  exit 1
}

assert_fixture_metrics() {
  local body
  body=$(curl -sf --max-time 5 "${BASE_URL}/metrics")
  echo "${body}" | grep -q 'it_pod_info{' || return 1
  echo "${body}" | grep -q 'controller_kind="Deployment"' || return 1
  echo "${body}" | grep -q 'controller_name="fixture-web"' || return 1
  echo "${body}" | grep -q 'namespace="metadata-exporter-it"' || return 1
  # Flatten + sanitize: Deployment annotation "integration.test/controller-note"
  # becomes Prometheus label "controller_annotation_integration_test_controller_note".
  echo "${body}" \
    | grep 'it_pod_info{' \
    | grep -q 'controller_annotation_integration_test_controller_note="from-fixture-deployment"' \
    || return 1
  # Multi-container: one series per container, each carrying the container's image tag.
  echo "${body}" \
    | grep 'it_pod_container_info{' \
    | grep 'container="pause-main"' \
    | grep -q 'image="registry.k8s.io/pause:3.9"' \
    || return 1
  echo "${body}" \
    | grep 'it_pod_container_info{' \
    | grep 'container="pause-sidecar"' \
    | grep -q 'image="registry.k8s.io/pause:3.10"' \
    || return 1
  return 0
}

log "waiting for expected series in /metrics (up to ${METRICS_WAIT_SEC}s)"
deadline=$((SECONDS + METRICS_WAIT_SEC))
backoff=1
while (( SECONDS < deadline )); do
  if assert_fixture_metrics; then
    log "metrics assertion passed"
    break
  fi
  sleep "${backoff}"
  if (( backoff < 8 )); then
    backoff=$((backoff * 2))
  fi
done
if ! assert_fixture_metrics; then
  log "error: timed out waiting for it_pod_info series for fixture-web"
  curl -sf --max-time 5 "${BASE_URL}/metrics" | head -c 8000 || true
  exit 1
fi

log "delete fixture deployment and expect series to disappear"
kubectl delete deployment/fixture-web -n "${NS}" --wait=true --timeout=120s

fixture_gone() {
  local body
  body=$(curl -sf --max-time 5 "${BASE_URL}/metrics") || return 1
  if echo "${body}" | grep -q 'controller_name="fixture-web"'; then
    return 1
  fi
  return 0
}

deadline=$((SECONDS + DELETE_WAIT_SEC))
backoff=1
while (( SECONDS < deadline )); do
  if fixture_gone; then
    log "delete propagation assertion passed"
    exit 0
  fi
  sleep "${backoff}"
  if (( backoff < 8 )); then
    backoff=$((backoff * 2))
  fi
done

log "error: metrics still mention fixture-web after delete"
curl -sf --max-time 5 "${BASE_URL}/metrics" | grep 'fixture-web' || true
exit 1
