#!/usr/bin/env bash
set -Eeuo pipefail

# Run from the recoveryguard-lab repository root.
# Usage: bash scripts/rg-00-leader-failure.sh <run-id> <topic> [output-dir]

RUN_ID="${1:-}"
TOPIC="${2:-}"
OUTPUT_DIR="${3:-runs}"
JAR="ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar"
FAILURE_ID="rg-00-${RUN_ID}-leader-kill"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-180}"

if [[ -z "$RUN_ID" || -z "$TOPIC" ]]; then
  echo "Usage: $0 <run-id> <topic> [output-dir]" >&2
  exit 2
fi

if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9._-]{1,80}$ ]]; then
  echo "Invalid run ID. Use 1-80 letters, digits, dots, underscores, or hyphens." >&2
  exit 2
fi

if ! [[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "TIMEOUT_SECONDS must be a positive integer." >&2
  exit 2
fi

if ! command -v jq >/dev/null 2>&1 || ! command -v timeout >/dev/null 2>&1; then
  echo "This script requires jq and GNU timeout." >&2
  exit 2
fi

MANIFEST="$OUTPUT_DIR/$RUN_ID/manifest.json"
if [[ ! -f "$MANIFEST" ]]; then
  echo "Run manifest not found: $MANIFEST" >&2
  exit 2
fi

manifest_run_id="$(jq -er '.runId | strings' "$MANIFEST")" || {
  echo "Run manifest has no valid runId." >&2
  exit 2
}
manifest_topic="$(jq -er '.topic | strings' "$MANIFEST")" || {
  echo "Run manifest has no valid topic." >&2
  exit 2
}
if [[ "$manifest_run_id" != "$RUN_ID" || "$manifest_topic" != "$TOPIC" ]]; then
  echo "Run manifest mismatch. Run ID: $manifest_run_id; topic: $manifest_topic." >&2
  echo "Refusing to stop any broker." >&2
  exit 2
fi

if [[ ! -f "$JAR" ]]; then
  echo "Ground-truth JAR not found. Build it first: mvn -f ground-truth/pom.xml clean verify" >&2
  exit 2
fi

failure_started=0
broker_stopped=0
failure_ended=0
failed_service=""
deadline=0

ground_truth() {
  java -jar "$JAR" "$@" --output-dir "$OUTPUT_DIR"
}

cleanup() {
  local exit_code=$?
  trap - EXIT
  if [[ "$broker_stopped" -eq 1 ]]; then
    echo "Safety cleanup: starting $failed_service..." >&2
    docker compose start "$failed_service" >/dev/null 2>&1 || true
  fi
  if [[ "$failure_started" -eq 1 && "$failure_ended" -eq 0 ]]; then
    ground_truth failure-end \
      --run-id "$RUN_ID" \
      --failure-id "$FAILURE_ID" \
      --result failed \
      --details "RG-00 script exited before recovery completed" >/dev/null 2>&1 || true
  fi
  exit "$exit_code"
}
trap cleanup EXIT

running_services() {
  docker compose ps --status running --services
}

describe_topic() {
  local service remaining
  while IFS= read -r service; do
    [[ -n "$service" ]] || continue
    remaining=$((deadline - SECONDS))
    (( remaining > 0 )) || return 1
    if timeout --signal=TERM "${remaining}s" docker compose exec -T "$service" \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$service:19092" --describe --topic "$TOPIC" 2>/dev/null; then
      return 0
    fi
  done < <(running_services)
  return 1
}

partition_state() {
  describe_topic | awk '
    /Partition:/ {
      for (i = 1; i <= NF; i++) {
        if ($i == "Leader:") leader = $(i + 1)
        if ($i == "Replicas:") replicas = $(i + 1)
        if ($i == "Isr:") isr = $(i + 1)
      }
      count++
    }
    END {
      if (count != 1 || leader == "" || replicas == "" || isr == "") exit 1
      print leader, replicas, isr
    }'
}

service_for_broker() {
  case "$1" in
    1) echo "kafka-1" ;;
    2) echo "kafka-2" ;;
    3) echo "kafka-3" ;;
    *) return 1 ;;
  esac
}

isr_count() {
  local value="$1"
  if [[ -z "$value" ]]; then
    echo 0
  else
    awk -F, '{ print NF }' <<< "$value"
  fi
}

echo "Checking RG-00 preconditions..."
deadline=$((SECONDS + TIMEOUT_SECONDS))
state="$(partition_state)" || {
  echo "Could not read exactly one partition for topic $TOPIC within the timeout." >&2
  exit 1
}
read -r leader replicas isr <<< "$state"

if [[ "$replicas" != "1,2,3" && "$replicas" != "1,3,2" && "$replicas" != "2,1,3" && "$replicas" != "2,3,1" && "$replicas" != "3,1,2" && "$replicas" != "3,2,1" ]]; then
  echo "Expected replicas 1,2,3. Found: $replicas" >&2
  exit 1
fi
if [[ "$(isr_count "$isr")" -ne 3 ]]; then
  echo "Expected ISR=3 before failure. Found: $isr" >&2
  exit 1
fi

failed_service="$(service_for_broker "$leader")" || {
  echo "Leader broker ID is not mapped to this Lab: $leader" >&2
  exit 1
}

echo "Partition leader: broker $leader ($failed_service)"
echo "Recording failure start..."
ground_truth failure-start \
  --run-id "$RUN_ID" \
  --failure-id "$FAILURE_ID" \
  --target "$failed_service" \
  --action container-kill \
  --details "SIGKILL the active partition leader during the producer workload"
failure_started=1

echo "Killing active leader..."
# Set this first. Cleanup must restart the broker even if kill is interrupted.
broker_stopped=1
docker compose kill "$failed_service"

echo "Waiting for a different partition leader..."
deadline=$((SECONDS + TIMEOUT_SECONDS))
new_leader=""
while (( SECONDS < deadline )); do
  if state="$(partition_state 2>/dev/null)"; then
    read -r current_leader current_replicas current_isr <<< "$state"
    if [[ "$current_leader" != "$leader" && "$current_leader" != "-1" ]]; then
      new_leader="$current_leader"
      break
    fi
  fi
  sleep 2
done

if [[ -z "$new_leader" ]]; then
  echo "No replacement leader elected within $TIMEOUT_SECONDS seconds." >&2
  exit 1
fi
echo "Replacement leader: broker $new_leader"

echo "Restarting $failed_service..."
docker compose start "$failed_service"
broker_stopped=0

echo "Waiting for all three replicas to return to ISR..."
deadline=$((SECONDS + TIMEOUT_SECONDS))
recovered=0
while (( SECONDS < deadline )); do
  if state="$(partition_state 2>/dev/null)"; then
    read -r current_leader current_replicas current_isr <<< "$state"
    if [[ "$current_leader" != "-1" && "$(isr_count "$current_isr")" -eq 3 ]]; then
      recovered=1
      break
    fi
  fi
  sleep 2
done

if [[ "$recovered" -ne 1 ]]; then
  echo "ISR did not return to 3 within $TIMEOUT_SECONDS seconds." >&2
  exit 1
fi

ground_truth failure-end \
  --run-id "$RUN_ID" \
  --failure-id "$FAILURE_ID" \
  --result completed \
  --details "Replacement leader elected; original broker restarted; ISR returned to three replicas"
failure_ended=1

echo "RG-00 failure injection completed."
echo "Leader before: broker $leader"
echo "Leader during recovery: broker $new_leader"
echo "Final ISR: $current_isr"
echo "Next: wait for the producer to finish, then run the ground-truth verifier."
