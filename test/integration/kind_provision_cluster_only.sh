#!/usr/bin/env bash
# Kind provisioning (cluster + kubeconfig only).
# Designed to be `source`d by other scripts so KUBECONFIG is available to
# the caller's shell.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${ROOT}"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-metadata-exporter-it}"

# Default to "keep cluster" (matches user request). If you want cleanup,
# export SKIP_CLUSTER_DELETE= (empty) before invoking.
if [[ -z "${SKIP_CLUSTER_DELETE:-}" ]]; then
  SKIP_CLUSTER_DELETE=1
fi

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: required command not found: $1" >&2
    exit 1
  }
}

need_cmd kind
need_cmd kubectl
need_cmd docker

CREATED_CLUSTER=0
KUBECONFIG_IT_FILE=""

kind_provision_cleanup() {
  # Only cleanup if we created the cluster and the caller didn't ask to keep it.
  if [[ "${CREATED_CLUSTER}" -eq 1 ]] && [[ -z "${SKIP_CLUSTER_DELETE:-}" ]]; then
    if [[ -n "${KUBECONFIG_IT_FILE}" ]] && [[ -f "${KUBECONFIG_IT_FILE}" ]]; then
      KUBECONFIG="${KUBECONFIG_IT_FILE}" kind delete cluster --name "${KIND_CLUSTER_NAME}" || true
    else
      kind delete cluster --name "${KIND_CLUSTER_NAME}" || true
    fi
  fi

  if [[ -n "${KUBECONFIG_IT_FILE}" ]]; then
    rm -f "${KUBECONFIG_IT_FILE}" || true
  fi
}

if [[ -z "${SKIP_KIND_CREATE:-}" ]]; then
  # shellcheck source=kind_ensure_cluster.inc.sh
  source "${SCRIPT_DIR}/kind_ensure_cluster.inc.sh"
  kind_integration_ensure_cluster || exit 1
else
  echo "SKIP_KIND_CREATE set: reusing existing kubeconfig context (cluster will not be deleted unless created by this script)" >&2
fi

# If executed directly (not sourced), ensure cleanup runs.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  trap kind_provision_cleanup EXIT
fi

