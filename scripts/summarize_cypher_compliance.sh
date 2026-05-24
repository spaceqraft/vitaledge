#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <compliance-log-file>" >&2
  exit 2
fi

log_file="$1"
if [[ ! -f "$log_file" ]]; then
  echo "error: log file not found: $log_file" >&2
  exit 2
fi

echo "Cypher Compliance Summary"
echo "log: $log_file"
echo

scenario_total=$(grep -Eo '[0-9]+ scenarios' "$log_file" | tail -n 1 | awk '{print $1}')
step_total=$(grep -Eo '[0-9]+ steps' "$log_file" | tail -n 1 | awk '{print $1}')
scenario_breakdown=$(grep -E '[0-9]+ scenarios \(' "$log_file" | tail -n 1 || true)
step_breakdown=$(grep -E '[0-9]+ steps \(' "$log_file" | tail -n 1 || true)

if [[ -n "${scenario_total:-}" ]]; then
  echo "scenarios: $scenario_total"
fi
if [[ -n "${step_total:-}" ]]; then
  echo "steps: $step_total"
fi
if [[ -n "$scenario_breakdown" ]]; then
  echo "$scenario_breakdown" | sed 's/^[[:space:]]*//'
fi
if [[ -n "$step_breakdown" ]]; then
  echo "$step_breakdown" | sed 's/^[[:space:]]*//'
fi

undefined_steps=$(grep -c 'step is undefined:' "$log_file" || true)
failed_steps=$(grep -c '^      Error:' "$log_file" || true)
failed_scenarios=$(grep -Ec '^--- FAIL: TestCypherCompliance/' "$log_file" || true)

if [[ -n "$scenario_breakdown" ]]; then
  parsed_failed_scenarios=$(echo "$scenario_breakdown" | grep -Eo '[0-9]+ failed' | head -n 1 | awk '{print $1}')
  parsed_undefined_scenarios=$(echo "$scenario_breakdown" | grep -Eo '[0-9]+ undefined' | head -n 1 | awk '{print $1}')
else
  parsed_failed_scenarios=""
  parsed_undefined_scenarios=""
fi

if [[ -n "$step_breakdown" ]]; then
  parsed_failed_steps=$(echo "$step_breakdown" | grep -Eo '[0-9]+ failed' | head -n 1 | awk '{print $1}')
  parsed_undefined_steps=$(echo "$step_breakdown" | grep -Eo '[0-9]+ undefined' | head -n 1 | awk '{print $1}')
else
  parsed_failed_steps=""
  parsed_undefined_steps=""
fi

if [[ -n "$parsed_failed_scenarios" ]]; then
  failed_scenarios="$parsed_failed_scenarios"
fi
if [[ -n "$parsed_failed_steps" ]]; then
  failed_steps="$parsed_failed_steps"
fi
if [[ -n "$parsed_undefined_steps" ]]; then
  undefined_steps="$parsed_undefined_steps"
fi

printf 'undefined steps: %s\n' "$undefined_steps"
printf 'failed scenario entries: %s\n' "$failed_scenarios"
printf 'failed step details: %s\n' "$failed_steps"
if [[ -n "$parsed_undefined_scenarios" ]]; then
  printf 'undefined scenarios: %s\n' "$parsed_undefined_scenarios"
fi

echo
echo "Top Undefined Steps"
if [[ "$undefined_steps" -eq 0 ]]; then
  echo "none"
else
  grep 'step is undefined:' "$log_file" \
    | sed 's/^.*step is undefined: //' \
    | sort \
    | uniq -c \
    | sort -nr \
    | head -n 15
fi

echo
echo "Top Failure Reasons"
if [[ "$failed_steps" -eq 0 ]]; then
  echo "none"
else
  grep '^      Error:' "$log_file" \
    | sed 's/^      Error: //' \
    | sort \
    | uniq -c \
    | sort -nr \
    | head -n 20
fi
