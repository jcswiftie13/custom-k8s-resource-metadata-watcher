#!/usr/bin/env bash
# Run the informer integration test on a Kind cluster.
#
# This script:
#   1) Provisions Kind cluster + temp KUBECONFIG (via a shared script)
#   2) Runs only informer test(s), with E2E_SKIP_SETUP=1 (no exporter rollout wait)
#
# Environment variables:
#   KIND_CLUSTER_NAME   kind cluster name (default metadata-exporter-it)
#   SKIP_KIND_CREATE    when set, reuse existing Kind cluster
#   SKIP_CLUSTER_DELETE when set together with create, keep the cluster
#                        (default in kind_provision_cluster_only.sh is keep)
#   INFORMER_TEST_PKG  go test package (default ./test/integration/e2e/...)
#   INFORMER_TEST_RUN  -run regex (default ^TestInformer_PodPendingImageUpdateNotifies$)
#   GOTEST_FLAGS        additional flags passed to go test
#
# Examples:
#   ./test/integration/run_informer_test.sh
#   SKIP_KIND_CREATE=1 ./test/integration/run_informer_test.sh
#   SKIP_KIND_CREATE=1 INFORMER_TEST_RUN='TestInformer_' ./test/integration/run_informer_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${ROOT}"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-metadata-exporter-it}"

# Provision: source so KUBECONFIG is exported to this shell.
source "${SCRIPT_DIR}/kind_provision_cluster_only.sh"
trap kind_provision_cleanup EXIT

export E2E_SKIP_SETUP=1
export E2E_REPO_ROOT="${ROOT}"

INFORMER_TEST_PKG="${INFORMER_TEST_PKG:-./test/integration/e2e/...}"
INFORMER_TEST_RUN="${INFORMER_TEST_RUN:-^TestInformer_PodPendingImageUpdateNotifies$}"

echo "Running informer test: pkg=${INFORMER_TEST_PKG} run=${INFORMER_TEST_RUN}"

# shellcheck disable=SC2086
/usr/local/go/bin/go test -tags integration -v -count=1 -timeout=20m -run "${INFORMER_TEST_RUN}" ${GOTEST_FLAGS:-} "${INFORMER_TEST_PKG}"

