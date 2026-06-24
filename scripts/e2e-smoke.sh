#!/usr/bin/env bash
# scripts/e2e-smoke.sh
#
# Post-deploy smoke test. Hits the public endpoints on the deployed
# aggregator and asserts the wire shape the /analytics page consumes.
#
# Usage:
#   scripts/e2e-smoke.sh https://portfolio-analytics-aggregator.onrender.com
#
# Exits non-zero on the first failure so CI can gate a release.

set -euo pipefail

BASE="${1:-${ANALYTICS_API_URL:-http://localhost:8093}}"
BASE="${BASE%/}"

red()   { printf '\033[31m%s\033[0m\n' "$1"; }
green() { printf '\033[32m%s\033[0m\n' "$1"; }
blue()  { printf '\033[34m%s\033[0m\n' "$1"; }

require_jq() {
  if ! command -v jq >/dev/null 2>&1; then
    red "jq is required; install via 'brew install jq' or apt-get."
    exit 2
  fi
}

check() {
  local name="$1"; local url="$2"; local jq_path="$3"
  blue "→ $name"
  local resp
  if ! resp=$(curl -sS --fail --max-time 30 "$url"); then
    red "  HTTP request failed: $url"
    exit 1
  fi
  if ! echo "$resp" | jq -e "$jq_path" >/dev/null 2>&1; then
    red "  Response missing jq path: $jq_path"
    echo "$resp" | head -c 400
    exit 1
  fi
  green "  OK"
}

require_jq

echo "Smoke-testing aggregator at: $BASE"
echo

# 1. Liveness — Spring actuator is on by default.
check "actuator health" "$BASE/actuator/health" '.status == "UP"'

# 2. Public markers endpoint returns an array (may be empty pre-backfill).
check "GET /api/public/visits/markers?days=30" \
      "$BASE/api/public/visits/markers?days=30" \
      'type == "array"'

# 3. Public summary endpoint returns the expected composite shape.
check "GET /api/public/visits/summary?days=30" \
      "$BASE/api/public/visits/summary?days=30" \
      '.siteId and .days and (.totals | type == "object") and (.topCountries | type == "array")'

green "All smoke checks passed."
