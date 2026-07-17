#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-audit}"
PROJECT_ID="${PROJECT_ID:-portfolio-notify-prod}"
REGION="${REGION:-us-central1}"
AGENT_DAILY_BUDGET_USD="${AGENT_DAILY_BUDGET_USD:-0.75}"

if [[ "${MODE}" != "audit" && "${MODE}" != "apply" ]]; then
  echo "Usage: $0 [audit|apply]" >&2
  exit 2
fi

# service|min instances|max instances|CPU throttling (true, false, or preserve)
SERVICE_POLICIES=(
  "knowledge-service|0|1|true"
  "portfolio-admin-service|0|1|true"
  "portfolio-agent-service|1|1|true"
  "portfolio-analytics-aggregator|0|1|preserve"
  "portfolio-analytics-alerts|0|1|preserve"
  "portfolio-mcp-gateway|0|1|true"
  "portfolio-mcp-server|0|1|true"
  "portfolio-metrics-scraper|0|1|true"
  "portfolio-notification-service|0|1|true"
  "portfolio-rag-indexer|0|1|preserve"
  "portfolio-search-indexer|0|1|preserve"
)

drift=0

describe_service() {
  gcloud run services describe "$1" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --format=json
}

audit_service() {
  local service="$1"
  local expected_min="$2"
  local expected_max="$3"
  local expected_cpu="$4"
  local document actual_min actual_max actual_cpu

  document="$(describe_service "${service}")"
  actual_min="$(jq -r '.spec.template.metadata.annotations["autoscaling.knative.dev/minScale"] // "0"' <<<"${document}")"
  actual_max="$(jq -r '.spec.template.metadata.annotations["autoscaling.knative.dev/maxScale"] // "unset"' <<<"${document}")"
  actual_cpu="$(jq -r '.spec.template.metadata.annotations["run.googleapis.com/cpu-throttling"] // "true"' <<<"${document}")"

  if [[ "${actual_min}" != "${expected_min}" || "${actual_max}" != "${expected_max}" ]]; then
    echo "DRIFT ${service}: scale=${actual_min}..${actual_max}, expected=${expected_min}..${expected_max}"
    return 1
  elif [[ "${expected_cpu}" != "preserve" && "${actual_cpu}" != "${expected_cpu}" ]]; then
    echo "DRIFT ${service}: cpu-throttling=${actual_cpu}, expected=${expected_cpu}"
    return 1
  else
    echo "OK    ${service}: scale=${actual_min}..${actual_max}, cpu-throttling=${actual_cpu}"
    return 0
  fi
}

apply_service() {
  local service="$1"
  local expected_min="$2"
  local expected_max="$3"
  local expected_cpu="$4"
  local cpu_flag=()

  if [[ "${expected_cpu}" == "true" ]]; then
    cpu_flag=(--cpu-throttling)
  elif [[ "${expected_cpu}" == "false" ]]; then
    cpu_flag=(--no-cpu-throttling)
  fi

  echo "APPLY ${service}: scale=${expected_min}..${expected_max}, cpu=${expected_cpu}"
  gcloud run services update "${service}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --min-instances="${expected_min}" \
    --max-instances="${expected_max}" \
    "${cpu_flag[@]}" \
    --quiet
}

for policy in "${SERVICE_POLICIES[@]}"; do
  IFS='|' read -r service expected_min expected_max expected_cpu <<<"${policy}"
  if audit_service "${service}" "${expected_min}" "${expected_max}" "${expected_cpu}"; then
    continue
  fi
  if [[ "${MODE}" == "apply" ]]; then
    apply_service "${service}" "${expected_min}" "${expected_max}" "${expected_cpu}"
    if audit_service "${service}" "${expected_min}" "${expected_max}" "${expected_cpu}"; then
      continue
    fi
  fi
  drift=1
done

agent_document="$(describe_service portfolio-agent-service)"
actual_agent_budget="$(jq -r '.spec.template.spec.containers[0].env[]? | select(.name == "AGENT_CHAT_DAILY_BUDGET_USD") | .value // empty' <<<"${agent_document}")"

if [[ "${MODE}" == "apply" && "${actual_agent_budget}" != "${AGENT_DAILY_BUDGET_USD}" ]]; then
  gcloud run services update portfolio-agent-service \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --update-env-vars="AGENT_CHAT_DAILY_BUDGET_USD=${AGENT_DAILY_BUDGET_USD}" \
    --quiet
  actual_agent_budget="${AGENT_DAILY_BUDGET_USD}"
fi

if [[ "${actual_agent_budget}" != "${AGENT_DAILY_BUDGET_USD}" ]]; then
  echo "DRIFT portfolio-agent-service: daily AI budget=${actual_agent_budget:-unset}, expected=${AGENT_DAILY_BUDGET_USD}"
  drift=1
else
  echo "OK    portfolio-agent-service: daily AI budget=${actual_agent_budget} USD"
fi

if [[ "${drift}" -ne 0 ]]; then
  echo "Cloud Run cost policy drift detected." >&2
  exit 1
fi

echo "Cloud Run cost policy is compliant."
