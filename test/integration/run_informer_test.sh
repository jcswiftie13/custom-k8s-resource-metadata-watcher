#!/usr/bin/env bash
# Run a standalone informer observer for manual watch-behavior inspection.
#
# This script:
#   1) Provisions Kind cluster + temp KUBECONFIG (via a shared script)
#   2) Ensures the target namespace exists
#   3) Starts a long-running Pod informer with server-side field selector
#
# Environment variables:
#   KIND_CLUSTER_NAME    kind cluster name (default metadata-exporter-it)
#   SKIP_KIND_CREATE     when set, reuse existing Kind cluster
#   SKIP_CLUSTER_DELETE  when set together with create, keep the cluster
#                        (default in kind_provision_cluster_only.sh is keep)
#   OBSERVER_NAMESPACE   namespace to watch (default e2e-informer-pending-image-0)
#   OBSERVER_SELECTOR    field selector (default status.phase=Pending)
#   OBSERVER_KUBECONFIG  optional kubeconfig path for observer process
#
# Examples:
#   ./test/integration/run_informer_test.sh
#   SKIP_KIND_CREATE=1 ./test/integration/run_informer_test.sh
#   SKIP_KIND_CREATE=1 OBSERVER_SELECTOR='status.phase=Running' ./test/integration/run_informer_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${ROOT}"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-metadata-exporter-it}"
OBSERVER_NAMESPACE="${OBSERVER_NAMESPACE:-e2e-informer-pending-image-0}"
OBSERVER_SELECTOR="${OBSERVER_SELECTOR:-status.phase=Pending}"
PENDING_POD_MANIFEST="${ROOT}/test/integration/manifests/pending-pod.yaml"

# Provision: source so KUBECONFIG is exported to this shell.
source "${SCRIPT_DIR}/kind_provision_cluster_only.sh"
trap kind_provision_cleanup EXIT

kubectl get namespace "${OBSERVER_NAMESPACE}" >/dev/null 2>&1 || kubectl create namespace "${OBSERVER_NAMESPACE}" >/dev/null

echo "Starting informer observer"
echo "  namespace      : ${OBSERVER_NAMESPACE}"
echo "  fieldSelector  : ${OBSERVER_SELECTOR}"
echo "  kubeconfig     : ${OBSERVER_KUBECONFIG:-${KUBECONFIG:-<default>}}"
echo
echo "Try these in another terminal:"
echo "  kubectl -n ${OBSERVER_NAMESPACE} apply -f ${PENDING_POD_MANIFEST}"
echo "  kubectl -n ${OBSERVER_NAMESPACE} get pod informer-pending-image-pod -w"
echo "  # edit ${PENDING_POD_MANIFEST}, then apply again:"
echo "  kubectl -n ${OBSERVER_NAMESPACE} apply -f ${PENDING_POD_MANIFEST}"
echo

observer_args=(
  --namespace "${OBSERVER_NAMESPACE}"
  --field-selector "${OBSERVER_SELECTOR}"
)
if [[ -n "${OBSERVER_KUBECONFIG:-}" ]]; then
  observer_args+=(--kubeconfig "${OBSERVER_KUBECONFIG}")
fi

go run ./test/tools/informer-observer/informer_pending_observer.go "${observer_args[@]}"

