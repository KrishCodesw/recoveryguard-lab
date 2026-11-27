# Lab Operations Guide

This guide defines the operator workflow for the local RecoveryGuard Lab.

## 1. Prerequisites

- Docker Engine
- Docker Compose v2
- JDK 17
- Maven 3.9 or later
- Bash
- `jq`
- GNU `timeout` for the RG-00 failure script

The failure-injection scripts use Bash and GNU command-line tools. Confirm that these tools are available in the shell before running a scenario.

## 2. Start the cluster

From the repository root:

```bash
docker compose up -d --wait --wait-timeout 240
docker compose ps
```

The cluster uses these host endpoints:

- `localhost:29092`
- `localhost:39092`
- `localhost:49092`

Inside the Compose network, use the service endpoints on port `19092`.

## 3. Run the smoke test

```bash
bash scripts/smoke-test.sh
```

The smoke test checks broker availability, replication factor, ISR membership, and a basic produce/read operation. It is a cluster readiness check, not a recovery correctness test.

## 4. Build the ground-truth tool

```bash
mvn --batch-mode --no-transfer-progress -f ground-truth/pom.xml clean verify
```

The executable JAR is written to:

```text
ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar
```

## 5. Prepare a run

Use a new topic and unique run ID for every experiment. Set topic retention to a value that exceeds the expected run and verification duration. Do not use a compacted topic for the current record verifier workflow.

Create the topic using the scenario-specific partition count and replication settings. The recorder does not create or alter topics.

Create the run manifest before starting the producer:

```bash
java -jar ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar create \
  --output-dir runs \
  --run-id <run-id> \
  --topic <topic-name> \
  --bootstrap-servers localhost:29092 \
  --description "<scenario description>"
```

Use a unique run ID with letters, numbers, dots, underscores, or hyphens.

## 6. Produce the workload

```bash
java -jar ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar produce \
  --output-dir runs \
  --run-id <run-id> \
  --count <record-count> \
  --payload-bytes <payload-size>
```

Record the exact producer command and retain its output. If a producer process runs in the background, wait for it to finish before verification.

## 7. Failure injection

Follow the scenario-specific procedure. Record failure start before the fault action and record failure end after the component has returned or the scenario has reached its defined terminal state.

Do not inject an unreviewed failure into a shared or production Kafka cluster. The scripts are designed for the local disposable Compose environment.

For RG-00, use:

```bash
bash scripts/rg-00-leader-failure.sh <run-id> <topic-name>
```

The script checks the run manifest and topic state before it stops the active partition leader. It attempts to restart the broker if it exits while the broker is stopped.

## 8. Verify records

Run verification after the producer has completed and the scenario has reached the required state:

```bash
java -jar ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar verify \
  --output-dir runs \
  --run-id <run-id> \
  --report reports/<run-id>-verification.json \
  --timeout-ms 60000
```

Review the full JSON report. Do not interpret `INCONCLUSIVE` or `INCOMPLETE` as a pass or as proven data loss.

## 9. Preserve the run

Retain the run manifest, event ledger, producer output, verifier report, Compose configuration, Kafka logs, and any product Observer report. Keep generated run data outside Git.

## 10. Stop and reset

```bash
docker compose down
```

The broker log directories use a temporary filesystem. Container removal deletes broker data. Confirm that all required experiment evidence is saved before removing containers.

## 11. Operational safety

- Use this environment only for controlled tests.
- Do not connect production data.
- Do not expose plaintext listeners to untrusted networks.
- Do not run fault scripts against a cluster other than the intended Lab.
- Use a new topic and run ID for each experiment.
- Preserve failed and inconclusive runs. Do not overwrite them with a later successful run.
