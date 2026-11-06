#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose)
topic="recoveryguard-lab-smoke-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}-$$"
expected="recoveryguard-lab-smoke-message"

echo "Checking three-broker topic replication..."
"${compose[@]}" exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 \
  --create \
  --topic "$topic" \
  --partitions 3 \
  --replication-factor 3

description="$("${compose[@]}" exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 \
  --describe \
  --topic "$topic")"

printf '%s\n' "$description" | awk '
  /Partition:[[:space:]]/ {
    partition_count++
    replicas = ""
    isr = ""

    for (i = 1; i <= NF; i++) {
      if ($i == "Replicas:") replicas = $(i + 1)
      if ($i == "Isr:") isr = $(i + 1)
    }

    replica_count = split(replicas, replica_ids, ",")
    isr_count = split(isr, isr_ids, ",")

    if (replica_count != 3 || isr_count != 3) exit 1

    for (i = 1; i <= 3; i++) {
      for (j = i + 1; j <= 3; j++) {
        if (replica_ids[i] == replica_ids[j]) exit 1
        if (isr_ids[i] == isr_ids[j]) exit 1
      }
    }
  }

  END {
    if (partition_count != 3) exit 1
  }
'

echo "All three partitions have three replicas and three in-sync replicas."

echo "Checking produce/consume..."
printf '%s\n' "$expected" | "${compose[@]}" exec -T kafka-1 \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:19092 \
  --topic "$topic" \
  --producer-property acks=all

actual="$("${compose[@]}" exec -T kafka-1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:19092 \
  --topic "$topic" \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 15000)"

if [[ "$actual" != "$expected" ]]; then
  echo "Expected '$expected' but received '$actual'." >&2
  exit 1
fi

echo "Kafka smoke test passed."
