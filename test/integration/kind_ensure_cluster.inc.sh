# Shared Kind provisioning helpers for integration scripts.
# Source from run.sh or kind_provision_cluster_only.sh after:
#   - SCRIPT_DIR is set
#   - KIND_CLUSTER_NAME is set
#   - SKIP_KIND_CREATE is honored (when set, this file does nothing)
#
# On success (when not SKIP_KIND_CREATE), sets:
#   CREATED_CLUSTER  0 = reused existing cluster matching config
#                    1 = created in this invocation (fresh or recreated)
#   KUBECONFIG_IT_FILE, export KUBECONFIG
#
# Expected node count is derived from "${SCRIPT_DIR}/kind-config.yaml"
# (lines matching "  - role:").

kind_expected_node_count() {
  local cfg="${SCRIPT_DIR}/kind-config.yaml"
  if [[ ! -f "${cfg}" ]]; then
    echo "error: kind config not found: ${cfg}" >&2
    return 1
  fi
  grep -E '^[[:space:]]*- role:' "${cfg}" | wc -l | tr -d '[:space:]'
}

kind_node_count() {
  local kc="$1"
  KUBECONFIG="${kc}" kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d '[:space:]'
}

kind_write_temp_kubeconfig() {
  KUBECONFIG_IT_FILE="$(mktemp)"
  kind get kubeconfig --name "${KIND_CLUSTER_NAME}" >"${KUBECONFIG_IT_FILE}"
  export KUBECONFIG="${KUBECONFIG_IT_FILE}"
}

# Ensures ${KIND_CLUSTER_NAME} exists with the same node count as kind-config.yaml.
kind_integration_ensure_cluster() {
  if [[ -n "${SKIP_KIND_CREATE:-}" ]]; then
    return 0
  fi

  local expected
  expected="$(kind_expected_node_count)" || return 1
  if [[ -z "${expected}" ]] || [[ "${expected}" -lt 1 ]]; then
    echo "error: invalid expected node count from kind-config.yaml: '${expected}'" >&2
    return 1
  fi

  if ! kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
    echo "creating Kind cluster ${KIND_CLUSTER_NAME} (${expected} nodes per kind-config.yaml)" >&2
    kind create cluster --name "${KIND_CLUSTER_NAME}" \
      --config "${SCRIPT_DIR}/kind-config.yaml" --wait 5m
    CREATED_CLUSTER=1
    kind_write_temp_kubeconfig
    return 0
  fi

  local probe_kc actual
  probe_kc="$(mktemp)"
  kind get kubeconfig --name "${KIND_CLUSTER_NAME}" >"${probe_kc}"
  actual="$(kind_node_count "${probe_kc}")"
  actual="${actual:-0}"

  if [[ "${actual}" != "${expected}" ]]; then
    echo "Kind cluster ${KIND_CLUSTER_NAME} has ${actual} node(s); expected ${expected}. Waiting for nodes to settle..." >&2
    if KUBECONFIG="${probe_kc}" kubectl wait --for=condition=Ready nodes --all --timeout=3m 2>/dev/null; then
      actual="$(kind_node_count "${probe_kc}")"
      actual="${actual:-0}"
    fi
  fi

  if [[ "${actual}" == "${expected}" ]]; then
    echo "reusing Kind cluster ${KIND_CLUSTER_NAME} (${actual} nodes match kind-config.yaml)" >&2
    KUBECONFIG_IT_FILE="${probe_kc}"
    export KUBECONFIG="${KUBECONFIG_IT_FILE}"
    CREATED_CLUSTER=0
    return 0
  fi

  echo "Kind cluster ${KIND_CLUSTER_NAME}: ${actual} node(s) != expected ${expected}; deleting and recreating..." >&2
  rm -f "${probe_kc}"
  kind delete cluster --name "${KIND_CLUSTER_NAME}" || true
  kind create cluster --name "${KIND_CLUSTER_NAME}" \
    --config "${SCRIPT_DIR}/kind-config.yaml" --wait 5m
  CREATED_CLUSTER=1
  kind_write_temp_kubeconfig
}
