#!/usr/bin/env bash
# Dev container bootstrap: wait for ClickHouse, extract native router_check_tool,
# pull the Envoy tools image (for TestMatchRouterCheckScale + docker fallback).
set -euo pipefail

CH_HOST="${POC_CH_ADDR%%:*}"
CH_HTTP_PORT="${POC_CH_HTTP_PORT:-8123}"
ROUTERCHECK_IMAGE="${POC_ROUTERCHECK_IMAGE:-envoyproxy/envoy:tools-v1.34-latest}"

echo "Waiting for ClickHouse at ${CH_HOST}:${CH_HTTP_PORT}..."
for i in $(seq 1 90); do
  if curl -fsS "http://${CH_HOST}:${CH_HTTP_PORT}/ping" >/dev/null 2>&1; then
    echo "ClickHouse ready."
    break
  fi
  if [[ "$i" -eq 90 ]]; then
    echo "timeout waiting for ClickHouse (check: docker compose logs clickhouse)" >&2
    exit 1
  fi
  sleep 1
done

cd "${CONTAINER_WORKSPACE_FOLDER:-$(pwd)}/poc/route2a"
make routercheck-bin

docker pull "${ROUTERCHECK_IMAGE}"

if ! command -v claude >/dev/null 2>&1; then
  echo "Installing Claude Code CLI..."
  curl -fsSL https://claude.ai/install.sh | bash
fi

if command -v claude >/dev/null 2>&1; then
  echo "Claude Code CLI: $(claude --version)"
else
  echo "Claude Code CLI install finished but 'claude' is not on PATH." >&2
  echo "Add ~/.local/bin to PATH, then run: claude --version" >&2
  exit 1
fi

go mod tidy
make routercheck-bin

echo ""
echo "Dev container ready."
echo "  POC_CH_ADDR=${POC_CH_ADDR:-clickhouse:9000}"
echo "  POC_ROUTERCHECK_BIN=${POC_ROUTERCHECK_BIN:-$(pwd)/bin/router_check_tool}"
echo ""
echo "Quick checks:"
echo "  claude --version"
echo "  cd poc/route2a && go test -run TestMatchRouterCheckScale -v ."
echo "  cd poc/route2a && go test -run TestIPFlowClickHouse -v ."
echo "  cd poc/route2a && make bench-worst POC_GATEWAYS=20 POC_VS=5"
