#!/usr/bin/env bash
# HANDWRITE-BEGIN gap="missing-generator:e2e-test:defer-bounded-soak" tracker="#766" reason="Defer-specific fixed-task lifecycle workload and RSS plateau assertion."
# Defer bounded scheduled-task soak.
#
# Creates one successful task and one permanently failing task against Defer's
# own HTTP surface. The successful task must remain terminal while the failing
# task repeatedly exercises committed lease/nack/reschedule retry transitions.
# The task keyspace stays fixed so RSS, descriptor, thread/task, and p99 growth
# represent instability rather than expected scheduler-state growth.
# The default warmup adds at least 2,048 proposals (two control proposals per
# round) to the shared bounded 4,096-entry proposal cache, crossing the
# 1,024-entry snapshot cadence before measurement. After the warmup, batches
# of fixed-task rounds run until the snapshot's completed_proposals length
# stops growing, and a fresh control pair must leave that length unchanged.
# Each 30-second window's RSS is the median of the shared core sampler's
# samples, so one sample cannot decide drift.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFER_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$DEFER_DIR" && pwd)"
# service-observability comes from faberline/core: use the checkout cargo resolved.
SERVICE_OBSERVABILITY_DIR="$(cd "$REPO_ROOT" && cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "service-observability") | .manifest_path' | xargs dirname)"
source "$SERVICE_OBSERVABILITY_DIR/scripts/soak-metrics.sh"
DURATION_SECS="${DEFER_SOAK_DURATION_SECS:-60}"
UPSTREAM="${DEFER_UPSTREAM:-127.0.0.1:7141}"
RSS_GROWTH_PCT="${DEFER_SOAK_RSS_GROWTH_PCT:-10}"
FD_GROWTH="${DEFER_SOAK_FD_GROWTH:-8}"
TASK_GROWTH="${DEFER_SOAK_TASK_GROWTH:-4}"
P99_MS="${DEFER_SOAK_P99_MS:-250}"
P99_GROWTH_PCT="${DEFER_SOAK_P99_GROWTH_PCT:-100}"
WARMUP_ROUNDS="${DEFER_SOAK_WARMUP_ROUNDS:-1024}"
CACHE_BATCH_ROUNDS="${DEFER_SOAK_CACHE_BATCH_ROUNDS:-256}"
CACHE_MAX_BATCHES="${DEFER_SOAK_CACHE_MAX_BATCHES:-32}"
RSS_SAMPLE_SECS="${DEFER_SOAK_RSS_SAMPLE_SECS:-1}"
RSS_SAMPLER_TOKEN=""
QUEUE="${DEFER_SOAK_QUEUE:-soak-jobs}"
DEFER_PID="${DEFER_SOAK_PID:-}"
AUTOSTART_PID=""
AUTOSTART_DIR=""
SOAK_TMP_DIR=""
LATENCY_FILE="/dev/null"

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "!! required command not found: $1" >&2
    exit 1
  }
}

cleanup() {
  local ec=$?
  trap - EXIT INT TERM
  [[ -z "$RSS_SAMPLER_TOKEN" ]] || service_soak_rss_sampler_abort "$RSS_SAMPLER_TOKEN"
  if [[ -n "$AUTOSTART_PID" ]] && kill -0 "$AUTOSTART_PID" 2>/dev/null; then
    kill "$AUTOSTART_PID" 2>/dev/null || true
    wait "$AUTOSTART_PID" 2>/dev/null || true
  fi
  [[ -z "$AUTOSTART_DIR" ]] || rm -rf "$AUTOSTART_DIR"
  [[ -z "$SOAK_TMP_DIR" ]] || rm -rf "$SOAK_TMP_DIR"
  exit "$ec"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

for cmd in curl jq ps; do require "$cmd"; done

healthy() { curl -fsS --max-time 2 "http://${UPSTREAM}/healthz" >/dev/null 2>&1; }

if [[ "${DEFER_SOAK_AUTOSTART:-0}" == "1" ]]; then
  host="${UPSTREAM%:*}"
  port="${UPSTREAM##*:}"
  if (: </dev/tcp/"$host"/"$port") >/dev/null 2>&1 || healthy; then
    echo "!! isolation error: upstream ${UPSTREAM} is already occupied" >&2
    exit 1
  fi
  AUTOSTART_DIR="$(mktemp -d "${TMPDIR:-/tmp}/defer-soak.XXXXXX")"
  echo ">> building and launching isolated Defer at http://${UPSTREAM}"
  (cd "$REPO_ROOT" && cargo build -q -p defer --bin defer)
  "$REPO_ROOT/target/debug/defer" serve --bind "$UPSTREAM" --data-dir "$AUTOSTART_DIR/data" \
    >"$AUTOSTART_DIR/defer.log" 2>&1 &
  AUTOSTART_PID=$!
  DEFER_PID="$AUTOSTART_PID"
  for _ in $(seq 1 60); do
    if healthy; then
      break
    fi
    if ! kill -0 "$AUTOSTART_PID" 2>/dev/null; then
      echo "!! Defer exited before becoming healthy (log: $AUTOSTART_DIR/defer.log)" >&2
      tail -n 50 "$AUTOSTART_DIR/defer.log" >&2 || true
      exit 1
    fi
    sleep 1
  done
else
  if ! healthy; then
    echo "!! Defer upstream ${UPSTREAM} is not reachable" >&2
    echo "   set DEFER_UPSTREAM/DEFER_SOAK_PID or DEFER_SOAK_AUTOSTART=1" >&2
    exit 1
  fi
  if [[ -z "$DEFER_PID" ]]; then
    require pgrep
    DEFER_PID="$(pgrep -n '^defer$' || true)"
  fi
fi

if ! healthy; then
  echo "!! Defer upstream ${UPSTREAM} is not reachable" >&2
  exit 1
fi

[[ -n "$DEFER_PID" ]] || { echo "!! cannot determine Defer PID" >&2; exit 1; }

if ! [[ "$DURATION_SECS" =~ ^[0-9]+$ ]] || (( DURATION_SECS < 2 )); then
  echo "!! DEFER_SOAK_DURATION_SECS must be an integer >= 2" >&2
  exit 1
fi
if ! [[ "$RSS_GROWTH_PCT" =~ ^[0-9]+$ ]]; then
  echo "!! DEFER_SOAK_RSS_GROWTH_PCT must be a non-negative integer" >&2
  exit 1
fi
if ! [[ "$WARMUP_ROUNDS" =~ ^[0-9]+$ ]] || (( WARMUP_ROUNDS < 1 )); then
  echo "!! DEFER_SOAK_WARMUP_ROUNDS must be an integer >= 1" >&2
  exit 1
fi
if ! [[ "$CACHE_BATCH_ROUNDS" =~ ^[0-9]+$ ]] || (( CACHE_BATCH_ROUNDS < 1 )); then
  echo "!! DEFER_SOAK_CACHE_BATCH_ROUNDS must be an integer >= 1" >&2
  exit 1
fi
if ! [[ "$CACHE_MAX_BATCHES" =~ ^[0-9]+$ ]] || (( CACHE_MAX_BATCHES < 1 )); then
  echo "!! DEFER_SOAK_CACHE_MAX_BATCHES must be an integer >= 1" >&2
  exit 1
fi
if ! [[ "$RSS_SAMPLE_SECS" =~ ^[0-9]+$ ]] || (( RSS_SAMPLE_SECS < 1 )); then
  echo "!! DEFER_SOAK_RSS_SAMPLE_SECS must be an integer >= 1" >&2
  exit 1
fi
if (( DURATION_SECS / 2 < RSS_SAMPLE_SECS )); then
  echo "!! DEFER_SOAK_RSS_SAMPLE_SECS must not exceed the window length" >&2
  exit 1
fi
for value in "$FD_GROWTH" "$TASK_GROWTH" "$P99_MS" "$P99_GROWTH_PCT"; do
  [[ "$value" =~ ^[0-9]+$ ]] || {
    echo "!! Defer soak resource and latency limits must be non-negative integers" >&2
    exit 1
  }
done

echo ">> configure fixed queue, one successful task, and one retrying fault task"
curl -fsS --max-time 10 -X PUT "http://${UPSTREAM}/v1/queues/${QUEUE}" \
  -H 'content-type: application/json' \
  -d '{"max_in_flight":16,"max_dispatch_per_tick":16,"max_dispatches_per_second":1000,"max_burst_size":1000,"lease_ttl_ms":30000,"retry_backoff_ms":0}' |
  jq -e '.task_count == 0' >/dev/null
curl -fsS --max-time 10 -X POST "http://${UPSTREAM}/v1/queues/${QUEUE}/tasks" \
  -H 'content-type: application/json' \
  -d "{\"task_id\":\"fixed\",\"target\":{\"url\":\"http://${UPSTREAM}/healthz\",\"method\":\"GET\",\"headers\":{}},\"payload\":{\"kind\":\"soak\"},\"schedule_at\":\"2000-01-01T00:00:00Z\",\"priority\":10,\"max_attempts\":3}" \
  -o /dev/null

for _ in $(seq 1 100); do
  STATUS="$(curl -fsS --max-time 10 "http://${UPSTREAM}/v1/queues/${QUEUE}/tasks/fixed" | jq -r '.status')"
  [[ "$STATUS" == "Succeeded" ]] && break
  sleep 0.1
done
[[ "${STATUS:-}" == "Succeeded" ]] || { echo "!! fixed task did not succeed" >&2; exit 1; }

# A route that Defer does not expose returns a real HTTP 404. Keep max_attempts
# deliberately high and use this queue's explicit zero-delay retry policy so
# the same durable task record exercises retry progress in both measured
# windows without growing the task keyspace or reaching DLQ.
curl -fsS --max-time 10 -X POST "http://${UPSTREAM}/v1/queues/${QUEUE}/tasks" \
  -H 'content-type: application/json' \
  -d "{\"task_id\":\"fixed-retry\",\"target\":{\"url\":\"http://${UPSTREAM}/soak-fault\",\"method\":\"POST\",\"headers\":{}},\"payload\":{\"kind\":\"retry-soak\"},\"schedule_at\":\"2000-01-01T00:00:00Z\",\"priority\":10,\"max_attempts\":1000000}" \
  -o /dev/null

metric_value() {
  local metric="$1"
  curl -fsS --max-time 10 "http://${UPSTREAM}/metrics" |
    awk -v metric="$metric" '$1 == metric { print int($2); found = 1 } END { if (!found) exit 1 }'
}

for _ in $(seq 1 100); do
  RETRIES="$(metric_value defer_dispatch_retried_total)"
  (( RETRIES > 0 )) && break
  sleep 0.1
done
(( ${RETRIES:-0} > 0 )) || { echo "!! retry task made no committed retry progress" >&2; exit 1; }

control() {
  local state="$1"
  curl -fsS --max-time 10 -X POST "http://${UPSTREAM}/v1/queues/${QUEUE}/control" \
    -H 'content-type: application/json' -d "{\"state\":\"${state}\"}" |
    jq -e --arg state "$state" '.control_state == $state' >/dev/null
}

inspect_fixed() {
  curl -fsS --max-time 10 "http://${UPSTREAM}/v1/queues/${QUEUE}" |
    jq -e '.task_count == 2 and .terminal_count == 1' >/dev/null
  curl -fsS --max-time 10 "http://${UPSTREAM}/v1/queues/${QUEUE}/tasks/fixed" |
    jq -e '.status == "Succeeded"' >/dev/null
  curl -fsS --max-time 10 "http://${UPSTREAM}/v1/queues/${QUEUE}/tasks/fixed-retry" |
    jq -e '.status == "Scheduled" or ((.status | type) == "object" and (.status.Leased != null))' >/dev/null
  curl -fsS --max-time 10 "http://${UPSTREAM}/metrics" >/dev/null
  healthy
}

latency_probe() {
  curl -fsS --max-time 10 -o /dev/null -w '%{time_total}\n' \
    "http://${UPSTREAM}/v1/queues/${QUEUE}/tasks/fixed" >>"$LATENCY_FILE"
}

TOTAL_OPS=0
ERR_COUNT=0
load_round() {
  if control Paused; then TOTAL_OPS=$((TOTAL_OPS + 1)); else ERR_COUNT=$((ERR_COUNT + 1)); fi
  if inspect_fixed; then TOTAL_OPS=$((TOTAL_OPS + 5)); else ERR_COUNT=$((ERR_COUNT + 1)); fi
  if control Running; then TOTAL_OPS=$((TOTAL_OPS + 1)); else ERR_COUNT=$((ERR_COUNT + 1)); fi
  if latency_probe; then TOTAL_OPS=$((TOTAL_OPS + 1)); else ERR_COUNT=$((ERR_COUNT + 1)); fi
}
run_until() {
  local deadline="$1"
  while [[ $(date +%s) -lt "$deadline" ]]; do load_round; done
}

proposal_cache_len() {
  local auth=()
  [[ -z "${DEFER_TOKEN:-}" ]] || auth=(-H "Authorization: Bearer ${DEFER_TOKEN}")
  curl -fsS --max-time 30 ${auth[@]+"${auth[@]}"} "http://${UPSTREAM}/admin/backup" |
    jq -e '(.completed_proposals // []) | length'
}

CACHE_LEN=""
CACHE_PAIR=""
saturate_proposal_cache() {
  local prev cur before after batch saturated=0
  prev="$(proposal_cache_len)" || { echo "!! cannot read proposal cache length from /admin/backup" >&2; exit 1; }
  for (( batch = 1; batch <= CACHE_MAX_BATCHES; batch++ )); do
    for _ in $(seq 1 "$CACHE_BATCH_ROUNDS"); do load_round; done
    cur="$(proposal_cache_len)" || { echo "!! cannot read proposal cache length from /admin/backup" >&2; exit 1; }
    if (( cur <= prev )); then
      CACHE_LEN="$cur"
      saturated=1
      echo ">> proposal cache saturated at ${cur} entries after ${batch} extra batch(es)"
      break
    fi
    prev="$cur"
  done
  if (( saturated == 0 )); then
    echo "!! proposal cache did not plateau after ${CACHE_MAX_BATCHES} batches (last length ${prev})" >&2
    exit 1
  fi

  before="$(proposal_cache_len)" || { echo "!! cannot read proposal cache length from /admin/backup" >&2; exit 1; }
  control Paused || { echo "!! control pair failed" >&2; exit 1; }
  control Running || { echo "!! control pair failed" >&2; exit 1; }
  after="$(proposal_cache_len)" || { echo "!! cannot read proposal cache length from /admin/backup" >&2; exit 1; }
  if (( after != before )); then
    echo "!! a fresh control pair still grew the proposal cache (${before} -> ${after})" >&2
    exit 1
  fi
  CACHE_PAIR="${before} -> ${after}"
  inspect_fixed || { echo "!! fixed queue state changed during the cache check" >&2; exit 1; }
}

echo ">> warmup: ${WARMUP_ROUNDS} fixed-task rounds (at least $((WARMUP_ROUNDS * 2)) proposals toward the shared bounded 4,096-entry proposal cache; crosses one 1,024-entry snapshot interval; saturation is proven below)"
for _ in $(seq 1 "$WARMUP_ROUNDS"); do load_round; done
saturate_proposal_cache
TOTAL_OPS=0
ERR_COUNT=0
RETRY_START="$(metric_value defer_dispatch_retried_total)"
SOAK_TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/defer-soak-metrics.XXXXXX")"
LATENCY_A="$SOAK_TMP_DIR/latency-a.seconds"
LATENCY_B="$SOAK_TMP_DIR/latency-b.seconds"

HALF=$((DURATION_SECS / 2))
LATENCY_FILE="$LATENCY_A"
service_soak_rss_sampler_start "$DEFER_PID" "$HALF" "$RSS_SAMPLE_SECS" || { echo "!! cannot start RSS sampler" >&2; exit 1; }
RSS_SAMPLER_TOKEN="$SERVICE_SOAK_RSS_LAST_TOKEN"
START="$(date +%s)"          # after the sampler starts, so its window ends no later than ours
run_until $((START + HALF))
service_soak_rss_sampler_stop "$RSS_SAMPLER_TOKEN" || { echo "!! RSS window A is not valid" >&2; exit 1; }
RSS_SAMPLER_TOKEN=""
SUMMARY_A="$SERVICE_SOAK_RSS_LAST_SUMMARY"
FD_A="$(service_soak_fd_count "$DEFER_PID")"
TASK_A="$(service_soak_task_count "$DEFER_PID")"
P99_A="$(service_soak_p99_ms "$LATENCY_A")"
RETRY_A="$(metric_value defer_dispatch_retried_total)"

LATENCY_FILE="$LATENCY_B"
service_soak_rss_sampler_start "$DEFER_PID" "$HALF" "$RSS_SAMPLE_SECS" || { echo "!! cannot start RSS sampler" >&2; exit 1; }
RSS_SAMPLER_TOKEN="$SERVICE_SOAK_RSS_LAST_TOKEN"
START_B="$(date +%s)"        # after the sampler starts, so its window ends no later than ours
run_until $((START_B + HALF))
service_soak_rss_sampler_stop "$RSS_SAMPLER_TOKEN" || { echo "!! RSS window B is not valid" >&2; exit 1; }
RSS_SAMPLER_TOKEN=""
SUMMARY_B="$SERVICE_SOAK_RSS_LAST_SUMMARY"
FD_B="$(service_soak_fd_count "$DEFER_PID")"
TASK_B="$(service_soak_task_count "$DEFER_PID")"
P99_B="$(service_soak_p99_ms "$LATENCY_B")"
RETRY_B="$(metric_value defer_dispatch_retried_total)"

[[ "$FD_A" =~ ^[0-9]+$ && "$FD_B" =~ ^[0-9]+$ && \
  "$TASK_A" =~ ^[0-9]+$ && "$TASK_B" =~ ^[0-9]+$ && \
  "$P99_A" =~ ^[0-9]+$ && "$P99_B" =~ ^[0-9]+$ ]] || {
  echo "!! could not read Defer soak metrics for pid ${DEFER_PID}" >&2
  exit 1
}
GROWTH_PCT="$(service_soak_rss_window_growth_pct "$SUMMARY_A" "$SUMMARY_B")" || {
  echo "!! could not compute RSS window medians" >&2
  exit 1
}

echo ">> Defer soak report"
echo "   duration:     $((2 * HALF))s (two ${HALF}s steady windows)"
echo "   total_ops:    ${TOTAL_OPS}"
echo "   errors:       ${ERR_COUNT}"
echo "   cache_len:     ${CACHE_LEN}"
echo "   cache_pair:    ${CACHE_PAIR}"
echo "   rss_window_a: ${SUMMARY_A}"
echo "   rss_window_b: ${SUMMARY_B}"
echo "   steady_drift: ${GROWTH_PCT}% (median of window A -> median of window B)"
echo "   fd_window:    ${FD_A} -> ${FD_B}"
echo "   tasks_window: ${TASK_A} -> ${TASK_B}"
echo "   p99_window:   ${P99_A}ms -> ${P99_B}ms"
echo "   retry_window: ${RETRY_START} -> ${RETRY_A} -> ${RETRY_B}"

# <HANDWRITE gap="missing-generator:e2e-test:defer-soak-progress-contract" tracker="#2214" reason="Fail closed when the measured steady-state workload reports zero operations.">
(( TOTAL_OPS > 0 )) || { echo "!! Defer soak observed zero measured operations" >&2; exit 1; }
# </HANDWRITE>
(( ERR_COUNT == 0 )) || { echo "!! Defer soak errors observed" >&2; exit 1; }
(( RETRY_A > RETRY_START && RETRY_B > RETRY_A )) || {
  echo "!! Defer retry scheduler made no progress in one or both steady windows (${RETRY_START} -> ${RETRY_A} -> ${RETRY_B})" >&2
  exit 1
}
(( GROWTH_PCT <= RSS_GROWTH_PCT )) || {
  echo "!! Defer RSS drift ${GROWTH_PCT}% exceeds ${RSS_GROWTH_PCT}%" >&2
  exit 1
}
service_soak_assert_max_growth "file descriptor" "$FD_A" "$FD_B" "$FD_GROWTH"
service_soak_assert_max_growth "thread/task" "$TASK_A" "$TASK_B" "$TASK_GROWTH"
service_soak_assert_latency_plateau "$P99_A" "$P99_B" "$P99_MS" "$P99_GROWTH_PCT"
echo ">> Defer bounded scheduled-task soak PASS"
# HANDWRITE-END
