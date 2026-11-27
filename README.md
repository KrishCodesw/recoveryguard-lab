# RecoveryGuard Lab

**Independent Kafka recovery test and experiment environment**

RecoveryGuard Lab is the controlled engineering environment used to test Kafka recovery behaviour and to validate RecoveryGuard evidence against an independent source of truth.

The Lab contains the Kafka cluster definition, test workloads, failure-injection procedures, ground-truth recorder, and record verifier. It is not part of the RecoveryGuard product runtime.

> **Environment status:** The repository contains a three-broker Apache Kafka 4.4.0 KRaft test cluster and an independent ground-truth recorder. RG-00, an active partition-leader failure scenario, has completed a valid 10,000-record verification run. Other scenarios remain research and implementation work unless a run record states otherwise.

## 1. Lab responsibilities

- Provide a repeatable Kafka test topology.
- Apply controlled failures through explicit operator scripts.
- Record experiment configuration and operator failure events.
- Generate unique test records with an independent acknowledgement ledger.
- Verify confirmed writes after recovery.
- Preserve experiment reports and logs outside the product repository.
- Provide independent results for product integration tests.

## 2. Repository structure

| Path | Responsibility |
|---|---|
| `docker-compose.yml` | Three-broker KRaft cluster |
| `ground-truth/` | Producer, durable event ledger, failure recorder, and record verifier |
| `scripts/smoke-test.sh` | Basic cluster and replication smoke test |
| `scripts/rg-00-leader-failure.sh` | RG-00 active leader failure injection |
| `docs/stage-3.6-scenario-specification.md` | Scenario catalogue, evidence requirements, and experiment plan |
| `.github/workflows/` | Cluster smoke tests and ground-truth CI |

Run outputs belong in local `runs/` and `reports/` directories. Do not commit broker data, generated records, credentials, or run output.

## 3. Test cluster

The Compose file starts three Apache Kafka 4.4.0 nodes. Each node has broker and controller roles. The three nodes form a KRaft controller quorum.

| Setting | Value |
|---|---|
| Kafka image | `apache/kafka:4.4.0` |
| Nodes | 3 |
| Process roles | broker, controller |
| Controller quorum | 3 voters |
| Default replication factor | 3 |
| Minimum in-sync replicas | 2 |
| Listener security | PLAINTEXT for local testing |
| Broker log storage | Temporary filesystem |

This topology is for local development and controlled experiments. It is not a production deployment template. Plaintext listeners must not be exposed to untrusted networks or used with production data.

### Host endpoints

| Node | Host endpoint |
|---|---|
| kafka-1 | `localhost:29092` |
| kafka-2 | `localhost:39092` |
| kafka-3 | `localhost:49092` |

Inside the Compose network, use `kafka-1:19092`, `kafka-2:19092`, and `kafka-3:19092`.

### Start and check

Run from the repository root:

```bash
docker compose up -d --wait --wait-timeout 240
bash scripts/smoke-test.sh
```

The smoke test checks three replicas and three ISR members for each test partition. It then produces and reads a test record.

### Stop and reset

```bash
docker compose down
```

Broker data uses a temporary filesystem. It survives a container restart but is removed when the container is removed. A new cluster after container removal starts with clean broker data.

## 4. Ground-truth recorder

The `ground-truth/` Maven module is independent of product implementation. It provides:

- a Kafka producer for controlled workloads;
- unique record identities and payload hashes;
- a durable send-intent and acknowledgement ledger;
- operator-recorded failure events; and
- a record verifier that compares Kafka records with the external ledger.

The recorder must not import RecoveryGuard product classes or use a product verdict as its ground-truth result.

### Requirements

- JDK 17
- Maven 3.9 or later
- Docker Engine with Docker Compose
- Running Lab Kafka cluster
- A new, non-compacted topic for each experiment run
- Retention long enough to complete the experiment and verification
- Bash, `jq`, and GNU `timeout` for the failure-injection script

### Build

Run from the repository root:

```bash
mvn --batch-mode --no-transfer-progress -f ground-truth/pom.xml clean verify
```

Executable JAR:

```text
ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar
```

### Create a topic

Use a new topic for each run. The recorder does not create or alter Kafka topics.

```bash
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 \
  --create \
  --topic rg-00-run-001 \
  --partitions 1 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=86400000
```

### Create a run

```bash
java -jar ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar create \
  --output-dir runs \
  --run-id rg-00-run-001 \
  --topic rg-00-run-001 \
  --bootstrap-servers localhost:29092 \
  --description "RG-00 active leader failure"
```

This creates a run manifest and append-only `events.jsonl` ledger under `runs/<run-id>/`. The ledger is stored outside Kafka and outside the broker containers.

## 5. Workload and acknowledgement semantics

Example producer command:

```bash
java -jar ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar produce \
  --output-dir runs \
  --run-id rg-00-run-001 \
  --count 10000 \
  --payload-bytes 512 \
  --interval-ms 1
```

The confirmed-write workload uses:

- `acks=all`;
- `enable.idempotence=true`; and
- `retries=Integer.MAX_VALUE`.

Each record has a unique key and a SHA-256 payload hash. The recorder writes a durable send-intent batch before sending it. A successful producer callback is recorded as `ACK_CONFIRMED`, with topic, partition, offset, timestamp, and payload hash. Producer errors are recorded as `OUTCOME_UNKNOWN`. An unknown outcome must not be treated as a confirmed rejection or a confirmed write.

The actual throughput depends on host resources, broker load, payload size, and acknowledgement latency.

## 6. RG-00: active partition-leader failure

RG-00 uses one partition, replication factor 3, and `min.insync.replicas=2`. The injector discovers the current partition leader, records the failure start, kills that broker container, waits for a replacement leader, restarts the failed broker, and waits for ISR to return to three replicas.

Use a new topic and run ID for each experiment.

Start the producer in the background, then run the failure injector:

```bash
java -jar ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar produce \
  --output-dir runs \
  --run-id rg-00-run-001 \
  --count 10000 \
  --payload-bytes 512 \
  --interval-ms 1 \
  > rg-00-run-001-producer.log 2>&1 &
producer_pid=$!

bash scripts/rg-00-leader-failure.sh rg-00-run-001 rg-00-run-001
wait "$producer_pid"
```

The injector has safety checks for the run manifest, topic, partition count, replica set, ISR, and broker mapping. It attempts to restart the failed broker if the script exits while the broker is stopped. Do not stop the other brokers during RG-00.

Verify after the producer has finished:

```bash
java -jar ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar verify \
  --output-dir runs \
  --run-id rg-00-run-001 \
  --report reports/rg-00-run-001-verification.json \
  --timeout-ms 60000
```

Keep the manifest, ledger, producer log, verifier report, Kafka configuration, and relevant container logs with the experiment record.

## 7. Record verifier results

The verifier reads the topic from the beginning using manual partition assignment. It does not join a consumer group or commit offsets. It compares confirmed writes by record ID, payload hash, partition, and offset. It also reports unknown outcomes, duplicates, and records that are not in the ledger.

| Status | Meaning |
|---|---|
| `PASS` | Every confirmed write was found once at its recorded location with its expected payload hash |
| `INTEGRITY_FAILURE` | A confirmed write is missing, changed, duplicated, or has an inconsistent location or producer outcome |
| `INCONCLUSIVE` | At least one producer outcome remains unknown or unresolved |
| `INCOMPLETE` | The scan did not reach all captured end offsets, or the run has no confirmed writes |

A `PASS` applies to the topic scan and run configuration. It does not prove KRaft quorum health, replica placement, reassignment completion, or application-level correctness.

## 8. Ledger integrity

The event ledger uses a SHA-256 hash chain. It detects edits to existing events, reordering, and removal of an event from the middle of the chain.

A hash chain alone cannot detect removal of a complete final suffix unless the expected terminal hash or event count is stored independently. It cannot prevent an actor who can rewrite the complete ledger and recompute the chain. An incomplete final line is rejected.

The recorder synchronizes evidence files and their containing directory before it reports successful creation. The filesystem must support directory synchronization.

## 9. Lab and product boundary

- Lab owns failure injection and independent ground truth.
- Product owns Observer behaviour, evidence contracts, and verdict implementation.
- Lab must not import product implementation classes.
- Product must not import or depend on Lab code, images, packages, or build outputs.
- Any shared contract must be versioned, approved, and pinned.
- Lab results must be generated independently of product verdicts.

## 10. CI

The Lab workflows build and test the ground-truth module and run the three-broker Kafka smoke and record-verification tests. CI uses isolated topics and run identifiers. CI results validate only the test paths that the workflow executes.

## 11. Experiment records

For each experiment, retain:

- unique run ID and scenario ID;
- Kafka image and broker version;
- Compose configuration and cluster topology;
- topic configuration and partition layout;
- producer settings and workload size;
- failure target, action, and operator timestamps;
- product Observer version and configuration, when used;
- ground-truth ledger and verifier report;
- Kafka and container logs;
- run status, deviations, and interpretation.

Do not compare runs when their relevant configuration differs without recording that difference.

## 12. Scenario catalogue

See [Stage 3.6 Scenario Specification](docs/stage-3.6-scenario-specification.md) for the scenario catalogue, evidence gaps, execution order, and acceptance criteria.
