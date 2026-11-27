# RecoveryGuard Lab

RecoveryGuard Lab is an independent test environment for Kafka recovery experiments. It does not contain product code. The product repository must not import or depend on this repository.

## Local Kafka cluster

The Compose file starts three Apache Kafka 4.4.0 nodes. Each node has the broker and controller roles. All three nodes are members of the KRaft controller quorum.

The cluster uses plaintext listeners and test-only settings. It is for local development and controlled experiments. It is not a production deployment template.

### Start

Run these commands from the repository root:

```bash
docker compose up -d --wait --wait-timeout 240
```

### Verify

Run the smoke test from WSL or a Bash shell:

```bash
bash scripts/smoke-test.sh
```

The test creates a topic with three partitions and replication factor three. It checks that each partition has three replicas and three in-sync replicas. It then writes one record with `acks=all` and reads it back.

GitHub Actions runs the same smoke test for pull requests and pushes to `main`.

### Connect from the host

Use one or more of these bootstrap endpoints:

| Node | Host endpoint |
| --- | --- |
| kafka-1 | `localhost:29092` |
| kafka-2 | `localhost:39092` |
| kafka-3 | `localhost:49092` |

Inside the Compose network, use `kafka-1:19092`, `kafka-2:19092`, or `kafka-3:19092`.

### Stop

Stop the containers:

```bash
docker compose down
```

Broker data uses a temporary filesystem. It remains available when a container restarts, but it is removed when the container is removed. Run `docker compose down` and start the cluster again to get clean broker state.

## Lab boundaries

- Lab code and ground truth must stay independent of RecoveryGuard product implementation.
- The Lab must use only an approved, pinned verdict contract when a contract is introduced.
- Do not add product classes or product Maven dependencies to this repository.
- The cluster uses plaintext communication. Do not use it for production data or expose its ports to an untrusted network.
- The fixed cluster ID and temporary broker data directories are for this disposable test cluster. Container removal deletes broker data.


## Independent ground-truth recorder

The `ground-truth/` Maven module provides an independent producer, acknowledgement ledger, failure-event recorder, and Kafka record verifier. It is Lab code only. It must not import RecoveryGuard product classes or use a product verdict as ground truth.

### Requirements

- Java 17
- Maven 3.9 or newer
- The Lab Kafka cluster must be running
- A separate, non-compacted topic for each run
- Enough topic retention to complete the test and verification

### Build and test

Run from the repository root:

```bash
cd ground-truth
mvn --batch-mode clean verify
```

The executable JAR is:

```text
ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar
```

### Create a test topic

Run from the repository root. Use a new topic for each experiment run:

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 \
  --create \
  --topic rg-run-001 \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=86400000
```

The tool does not create or alter Kafka topics. Topic setup is a separate Lab operation.

### Create a run

Run from the `ground-truth/` directory:

```bash
java -jar target/ground-truth-0.1.0-SNAPSHOT.jar create \
  --output-dir ../runs \
  --run-id rg-001 \
  --topic rg-run-001 \
  --bootstrap-servers localhost:29092 \
  --description "RG-00 leader failure"
```

This creates a manifest and an append-only `events.jsonl` file under `runs/rg-001/`. The ledger is stored on the host, outside Kafka and outside broker containers.

### Produce records

```bash
java -jar target/ground-truth-0.1.0-SNAPSHOT.jar produce \
  --output-dir ../runs \
  --run-id rg-001 \
  --count 100000 \
  --payload-bytes 512 \
  --interval-ms 1 \
  > /tmp/rg-001-producer.log 2>&1 &
producer_pid=$!
```

Keep this producer process running while you inject the failure. Record the failure before you stop the broker. After the broker has restarted, wait for the producer to finish:

```bash
# Run the failure-start command below, then inject the failure.
# Restore the broker and run failure-end.
wait "$producer_pid"
```

Read the producer log if the command exits with an error:

```bash
cat /tmp/rg-001-producer.log
```

Producer settings are fixed for the confirmed-write workload:

- `acks=all`
- `enable.idempotence=true`
- `retries=Integer.MAX_VALUE`

Each record has a unique key and a SHA-256 payload hash. The tool records a durable send-intent batch before it sends that batch. A successful producer callback is written to the durable ledger as `ACK_CONFIRMED`, with topic, partition, offset, timestamp, and payload hash. Producer errors are recorded as `OUTCOME_UNKNOWN`; the tool does not guess that Kafka rejected the write.

`--interval-ms 0` sends at the maximum client rate. A positive value inserts a delay between records. The observed rate depends on the host, broker, payload, and acknowledgement latency.

### Record failure events

Record the start before injecting a failure:

```bash
java -jar target/ground-truth-0.1.0-SNAPSHOT.jar failure-start \
  --output-dir ../runs \
  --run-id rg-001 \
  --failure-id broker-2-stop \
  --target kafka-2 \
  --action container-stop \
  --details "Stop broker 2 during producer workload"
```

Then inject the failure. For example:

```bash
docker compose stop kafka-2
```

After the broker returns:

```bash
docker compose start kafka-2
```

Record the observed end:

```bash
java -jar target/ground-truth-0.1.0-SNAPSHOT.jar failure-end \
  --output-dir ../runs \
  --run-id rg-001 \
  --failure-id broker-2-stop \
  --result completed \
  --details "Broker 2 started; replica catch-up still requires verification"
```

These are operator event timestamps. They are not exact broker process timestamps. Kafka and container logs must be collected separately when precise failure times are needed.

### Verify after recovery

Run only after the producer has finished and the cluster has reached the state being tested:

```bash
java -jar target/ground-truth-0.1.0-SNAPSHOT.jar verify \
  --output-dir ../runs \
  --run-id rg-001 \
  --report reports/rg-001-verification.json \
  --timeout-ms 60000
```

The verifier:

- Reads all available topic partitions from the beginning.
- Uses manual partition assignment.
- Does not join a consumer group.
- Does not commit offsets.
- Compares confirmed writes by record ID, payload hash, partition, and offset.
- Checks unknown outcomes separately.
- Reports duplicate record IDs and records that are not in the ledger.
- Writes a JSON report outside the run directory.

Possible statuses:

| Status | Meaning |
|---|---|
| `PASS` | Every confirmed write was found once at its recorded location, with its expected payload hash. |
| `INTEGRITY_FAILURE` | A confirmed write is missing, changed, duplicated, or has an inconsistent location or producer outcome. |
| `INCONCLUSIVE` | One or more records have an unknown or unresolved producer outcome. The report shows whether each record was found. |
| `INCOMPLETE` | The topic scan did not reach all captured end offsets, or the run has no confirmed writes. |

A verifier report with `PASS` proves record preservation for this topic scan. It does not prove KRaft quorum health, replica placement, reassignment completion, or application-level correctness. Stage 4 will decide how those checks can use approved evidence.

### Inspect the ledger

```bash
java -jar target/ground-truth-0.1.0-SNAPSHOT.jar show-ledger \
  --output-dir ../runs \
  --run-id rg-001
```

The ledger uses a SHA-256 hash chain. It detects accidental edits, removed events, reordering, and incomplete final writes. It is not tamper-proof against an actor who can rewrite the complete file and recompute the chain.

### Run records and reports

Keep run data outside Git. The root `.gitignore` excludes `runs/`, `reports/`, and Maven build output. Do not upload test payloads or broker data to the product repository.
