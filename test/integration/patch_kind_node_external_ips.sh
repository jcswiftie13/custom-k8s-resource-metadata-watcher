#!/usr/bin/env bash
# Append a documentation ExternalIP (RFC 5737 TEST-NET-3) to each Node's
# status.addresses so integration tests can assert it_node_address.
# Idempotent: skips nodes that already report an ExternalIP.
#
# Disable with INTEGRATION_PATCH_NODE_EXTERNAL_IP=0 (recommended when
# SKIP_KIND_CREATE=1 against a long-lived cluster you do not want modified).
set -euo pipefail

if [[ "${INTEGRATION_PATCH_NODE_EXTERNAL_IP:-1}" == "0" ]]; then
  exit 0
fi

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: required command not found: $1" >&2
    exit 1
  }
}

need_cmd kubectl

# 203.0.113.0/24 is TEST-NET-3 (RFC 5737); safe non-routed documentation space.
base_ip() {
  local idx="$1"
  printf '203.0.113.%d' $((10 + idx))
}

idx=0
while read -r node; do
  [[ -z "${node}" ]] && continue
  name="${node#node/}"
  existing="$(kubectl get node "${name}" -o jsonpath='{.status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null || true)"
  if [[ -n "${existing}" ]]; then
    echo "patch_kind_node_external_ips: node ${name} already has ExternalIP=${existing}; skip"
    idx=$((idx + 1))
    continue
  fi
  addr="$(base_ip "${idx}")"
  echo "patch_kind_node_external_ips: appending ExternalIP ${addr} to node ${name}"
  if ! kubectl patch node "${name}" --subresource=status --type=json \
    -p "[{\"op\":\"add\",\"path\":\"/status/addresses/-\",\"value\":{\"type\":\"ExternalIP\",\"address\":\"${addr}\"}}]" \
    >/dev/null; then
    echo "error: kubectl patch node ${name} failed" >&2
    exit 1
  fi
  idx=$((idx + 1))
done < <(kubectl get nodes -o name | sort)
