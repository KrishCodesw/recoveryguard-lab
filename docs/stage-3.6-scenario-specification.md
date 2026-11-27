# Stage 3.6 — Controlled Failure Scenario Specification

**Status:** Draft for review  
**Date:** 2026-11-27  
**Scope:** Task 3.6.1 only. This document defines the experiments. It does not implement failure injection or change RecoveryGuard product behavior.

## 1. Objective

Stage 3.6 must test failures that occur during real Kafka recovery work. It must not stop at a broker restart test.

The central question is:

> After a failure and automated recovery, can an independent validator prove that acknowledged data, partition state, replica placement, and the required Kafka control-plane state are correct?

A successful process restart or a healthy broker count is not sufficient proof.

This document uses the term **confirmed write** only for a record for which the producer received a successful Kafka acknowledgement and the external ledger durably recorded that result. A producer timeout does not prove that Kafka rejected the write. Kafka documents that a network error can occur before or after the broker commits a record, so the result can be unknown to the producer.

## 2. Research basis and limits

These scenarios are grounded in documented Kafka and operator behavior:

- Kafka's design documentation describes ISR-based replication and the uncertainty that exists when a producer does not receive the acknowledgement response.
- Kafka 4.4 documents replica reassignment, reassignment throttles, reassignment verification, and the risk that a throttle below the incoming write rate can stop replication progress.
- Kafka 4.4 documents KRaft controller quorum requirements. A majority of controllers must be available. Kafka also states that combined broker/controller mode is intended for small environments and does not isolate the controller role.
- Kafka 4.4 supports Eligible Leader Replicas (ELR). ELR state can affect leader selection. New clusters use ELR by default from Kafka 4.1. The test must inspect the actual ELR state; it must not assume the old unclean-election path.
- Strimzi documents broker storage failures, failed volume mounts, PVC problems, and broker replacement as operational recovery cases.
- Kafka supports rack-aware replica assignment through `broker.rack`.

These sources prove that the mechanisms and failure classes exist. They do **not** prove that no other product can validate them. Do not claim that RecoveryGuard solves an unsolved problem until the product and research-gap review is complete.

## 3. Current Lab baseline

The current Lab repository has this configuration:

| Property | Current value |
|---|---|
| Kafka image | Apache Kafka 4.4.0 |
| Nodes | 3 |
| Process roles | `broker,controller` on all 3 nodes |
| KRaft quorum | 3 combined nodes |
| Default replication factor | 3 |
| Minimum ISR | 2 |
| Broker log path | `/tmp/kraft-combined-logs` |
| Storage | 512 MiB tmpfs per broker |
| Listener security | PLAINTEXT |
| Existing smoke test | 3 partitions, RF=3, ISR=3, one `acks=all` write and read |

### 3.1 What the current Lab can test

- Basic leader failover.
- Broker outage and return.
- A moderate continuous producer workload.
- Record verification against an external ledger.
- Producer acknowledgement uncertainty, if the test harness can drop the response path.
- ELR behavior, after a capability preflight confirms the active feature and the Admin API can expose the needed state.

### 3.2 What the current Lab cannot isolate

- **Controller-only quorum loss:** all nodes are combined broker/controllers. Stopping two nodes also removes two brokers. A separate profile with 3 dedicated controllers and at least 3 dedicated brokers is required.
- **Independent data-disk failure:** the current broker log path also holds the KRaft metadata log because `metadata.log.dir` is not set. The current tmpfs layout must not be filled or removed as a data-only disk test.
- **Real PVC replacement:** the current Compose Lab is not a Kubernetes/Strimzi deployment. This requires a separate Kubernetes profile.
- **Real availability-zone failure:** rack labels in one local machine are only a placement simulation. They do not reproduce independent physical or cloud failure domains.

Do not run a destructive storage test against the current shared log path. Do not use `docker compose down` as a routine recovery step during these tests; it removes the current broker data.

## 4. Common experiment rules

Every run must use the following controls.

### 4.1 Workload

- Use a unique topic for each run.
- Use a fixed partition count and replication factor recorded in the run manifest.
- Use `acks=all` and `enable.idempotence=true` for the confirmed-write workload.
- Set and record `min.insync.replicas`. The initial baseline is RF=3 and min ISR=2.
- Disable log compaction for test topics. Set retention long enough to finish verification. Verify records before retention can remove them.
- Use unique record IDs and payload hashes. Use a fixed partition or a deterministic key when the test needs a known partition.
- Record producer configuration, topic configuration, Kafka version, and broker configuration for every run.

### 4.2 External ground truth

The Lab must own the ground-truth system. It must not import RecoveryGuard classes, call RecoveryGuard verdict code, or use a RecoveryGuard verdict as an expected result.

The ledger must be stored outside Kafka and outside the broker containers. It must record an append-only event for each record:

- `SEND_INTENT`: record ID and payload hash created before the send.
- `ACK_CONFIRMED`: producer received success; include topic, partition, offset, timestamp, and producer result.
- `SEND_TIMEOUT`, `SEND_ERROR`, or `OUTCOME_UNKNOWN`: producer did not receive a confirmed success.
- Failure injector events: target node, action, start time, end time, and result.

Flush the ledger to durable host storage after each acknowledgement result. If the producer receives an acknowledgement but the Lab process stops before the ledger records it durably, classify that record as **unknown**. Do not classify it as a confirmed write.

The verifier must read the test topic from the beginning after recovery. It must not commit offsets. It must compare record IDs and payload hashes with the external ledger.

### 4.3 Result rules

| Ledger state | Record after recovery | Result |
|---|---|---|
| `ACK_CONFIRMED` | Present once with matching hash | Preserved |
| `ACK_CONFIRMED` | Missing | Data-integrity failure |
| `ACK_CONFIRMED` | Present with a different hash | Data-integrity failure |
| `ACK_CONFIRMED` | Present more than once | Duplicate; evaluate against the stated delivery contract |
| `OUTCOME_UNKNOWN` | Present | Record exists; producer outcome was unknown |
| `OUTCOME_UNKNOWN` | Missing | Inconclusive; do not call it acknowledged-write loss |
| `SEND_ERROR` before send acceptance | Missing | Expected rejection |

A run passes only when its stated safety conditions hold and the final state remains stable for the configured observation window. A run that has not completed reassignment, replica catch-up, or controller recovery is **incomplete**, not a successful recovery.

### 4.4 Evidence independence

The Lab ledger and verifier are the ground truth. RecoveryGuard is the system under test.

The Lab must record its expected result before it reads the RecoveryGuard verdict. A test result must not be changed to match the product output.

## 5. Scenario registry

The first six scenarios form the primary Stage 3.6 experiment set. RG-00 is a baseline. RG-01 to RG-05 cover the main recovery risks. RG-06 tests the limit of what an independent validator can infer from producer outcomes.

---

## RG-00 — Abrupt leader failure and normal ISR recovery

**Priority:** Baseline  
**Lab profile:** Current 3-node profile

### Question

Does an abrupt leader failure preserve confirmed writes and restore the expected replica state?

### Preconditions

- One partition, RF=3, min ISR=2.
- All three replicas are in the ISR.
- Producer uses `acks=all` and idempotence.
- External ledger is active.

### Sequence

1. Produce a fixed warm-up set and verify all records.
2. Record the current leader, ISR, replicas, and broker list.
3. Kill the leader process without a graceful shutdown.
4. Keep the producer workload active, subject to normal Kafka acknowledgement behavior.
5. Observe leader election and ISR changes.
6. Restart the failed broker.
7. Wait for the replica to catch up and rejoin the ISR.
8. Compare all confirmed writes with the topic contents.

### Expected safe result

- A new leader is elected from an eligible replica.
- No confirmed write is missing or has a changed payload hash.
- The recovered broker rejoins the correct replica set.
- The partition reaches the configured stable state.

### Failure signals

- A confirmed write is missing.
- A partition remains without a usable leader after the recovery window.
- The broker returns but the expected replica does not recover.
- The run reports success while replica recovery is still incomplete.

---

## RG-01 — High write load during broker outage and replica catch-up

**Priority:** Critical  
**Lab profile:** Current 3-node profile, subject to host capacity

### Real failure mechanism

A broker outage reduces the available ISR. The recovering follower must catch up while producers continue to write. CPU, disk, and network pressure can delay recovery and can cause further ISR changes.

### Question

Can Kafka restore the missing replica while the surviving brokers continue to process a high write rate, without losing confirmed writes or declaring recovery too early?

### Preconditions

- RF=3 and min ISR=2.
- All replicas start in the ISR.
- A workload baseline has been measured on the same host.
- Workload rates are defined as measured fractions of that baseline, not as invented enterprise throughput numbers.

### Sequence

1. Measure a stable baseline for throughput and producer latency.
2. Start continuous writes at a selected rate.
3. Abruptly stop one follower.
4. Keep the workload running while the ISR shrinks.
5. Restart the follower while writes continue.
6. Measure catch-up progress, ISR changes, producer latency, and recovery time.
7. Repeat at multiple workload levels, including a host-safe high-load level.
8. Verify every confirmed write and the final replica state.

### Expected safe result

- Confirmed writes remain present.
- The recovering follower makes measurable catch-up progress.
- The replica returns to the ISR.
- The final state remains stable during the observation window.

### Failure signals

- Confirmed writes are missing or changed.
- The recovering follower makes no progress within the configured deadline.
- ISR membership repeatedly shrinks and expands.
- The cluster is called recovered before the follower catches up.

### Measurements

Record producer throughput, p50/p95/p99 acknowledgement latency, broker CPU and memory, disk usage, network throughput, ISR membership, under-replicated partitions, replica fetcher lag where available, and time to restore the full ISR.

Do not describe a test as high-performance unless the measured workload and host limits are included in the report.

---

## RG-02 — Throttled replica reassignment plus a second broker failure

**Priority:** Critical  
**Lab profile:** Advanced profile with at least 6 brokers

### Real failure mechanism

Replica reassignment consumes network and disk capacity. Kafka documents that a throttle below the incoming write rate can prevent reassignment replication from making progress. A second broker failure during this operation can leave the cluster with fewer safe placement options.

### Question

Can the system detect incomplete or stalled reassignment and validate the final replica state after another broker fails during movement?

### Preconditions

- At least 6 broker nodes, so the test has destination capacity.
- RF=3, min ISR=2.
- A partition has three source replicas and at least one destination broker outside its current assignment.
- The reassignment plan and throttle settings are recorded.
- Continuous producer load is active.

### Sequence

1. Record the current assignment, ISR, leader, and target assignment.
2. Start reassignment for selected partitions.
3. Apply a controlled replication throttle.
4. Run a subtest where the throttle is below the measured incoming write rate. Verify whether lag stops decreasing.
5. Raise the throttle to a value that allows progress.
6. While reassignment is active and before completion, fail a broker that hosts a replica for an affected partition.
7. Continue observing reassignment, ISR, leadership, and producer results.
8. Restore the failed broker.
9. Allow reassignment to complete or record why it remains incomplete.
10. Verify the final assignment and all confirmed writes.

### Expected safe result

- The Lab can identify whether replica movement is making progress.
- The second failure does not cause loss of confirmed writes.
- No partition is reported as fully recovered while it has an incomplete reassignment or an unsafe replica state.
- The final assignment matches the recorded target only after Kafka reports completion.
- Temporary replication throttles are removed after completion.

### Failure signals

- Confirmed writes are missing or changed.
- Reassignment is stalled but reported as complete.
- A target replica is absent from the final assignment.
- A partition is declared recovered while under-replicated or still moving.
- Throttles remain after the operation has completed.

### Important constraint

The current 3-broker RF=3 Lab has no spare broker for a meaningful replica-movement test. Do not simulate this scenario by pretending that the 3-node smoke test is a reassignment test.

---

## RG-03 — Broker log-directory failure and storage replacement

**Priority:** High  
**Lab profile:** Separate-storage profile; Strimzi/PVC version is a later extension

### Real failure mechanism

A broker can fail because of a full disk, unavailable log directory, failed volume mount, or lost persistent volume. A replacement broker can become process-healthy before all of its replicas have been rebuilt.

### Question

Can the system distinguish broker process recovery from recovery of the broker's partition data?

### Preconditions

- Broker data logs and KRaft metadata logs must use separate paths.
- The data path must use a dedicated volume that can be failed or replaced without deleting the metadata path.
- The original volume must be retained for forensic comparison.
- The test must not fill the current shared `/tmp/kraft-combined-logs` directory.

### Sequence

1. Start with RF=3 and all replicas in the ISR.
2. Produce and verify a confirmed record set.
3. Inject a data-log-directory failure or make the broker's data volume unavailable.
4. Record offline partitions, broker state, and storage errors.
5. Restore the original volume in one run.
6. In a separate run, attach a clean replacement data volume while preserving the broker identity and metadata storage as required by the selected setup.
7. Observe replica rebuild and ISR restoration.
8. Verify all confirmed writes from the surviving replicas.

### Expected safe result

- The test identifies offline or unavailable replicas.
- A broker process being online does not count as data recovery.
- Replicas return only after the data has been restored or rebuilt.
- No confirmed write is missing from the recovered topic.

### Lab limitation

The current Compose profile puts the KRaft metadata log in the first broker log directory because `metadata.log.dir` is not configured. First create a separate metadata path and separate data path. A true PVC deletion/replacement test needs a Kubernetes/Strimzi profile and must be reported separately from a local Docker simulation.

---

## RG-04 — KRaft controller quorum loss while brokers are otherwise healthy

**Priority:** High  
**Lab profile:** 3 dedicated controllers plus at least 3 dedicated brokers

### Real failure mechanism

KRaft requires a controller majority. When the controller quorum is unavailable, metadata changes and controller-led recovery actions cannot proceed. Existing broker data paths may remain available, which can make the data plane appear partly healthy while the control plane is unavailable.

### Question

Can RecoveryGuard distinguish data-plane availability from control-plane recovery, and can it identify a broker failure that occurs while the controller quorum cannot elect or maintain an active controller?

### Preconditions

- Dedicated controller nodes; do not use combined broker/controller nodes for this isolation test.
- Three controllers, with separate broker nodes.
- RF=3 and min ISR=2 for test topics.
- Controller quorum status is captured independently.

### Sequence

1. Start with a healthy controller quorum and healthy partitions.
2. Produce and verify a confirmed record set.
3. Stop or isolate two of the three controllers.
4. Verify that the controller quorum has lost its majority.
5. Check which existing produce/consume operations still work. Record timeouts as outcomes, not as proof of data loss.
6. While the quorum is unavailable, fail a broker that leads a test partition.
7. Observe whether leadership and metadata changes are blocked.
8. Restore the controller quorum.
9. Observe leader election, ISR changes, and metadata convergence.
10. Verify all confirmed writes.

### Expected safe result

- The Lab records the control-plane outage separately from broker availability.
- A running broker or a successful request to an existing partition does not imply that controller recovery is complete.
- The partition is not reported as recovered while a required leader election or metadata update is blocked.
- After the quorum returns, the cluster reaches a stable and valid partition state.
- No confirmed write is missing.

### Lab limitation

The current 3-node combined cluster cannot isolate this failure. Kafka documents combined mode as a small-environment option and recommends dedicated controllers for critical deployments.

---

## RG-05 — ELR-aware leader recovery after multiple failures

**Priority:** Advanced  
**Lab profile:** Kafka 4.4.0; separate run manifest and ELR capability preflight

### Real failure mechanism

Kafka 4.x can use Eligible Leader Replicas (ELR). A replica outside the ISR can still be an eligible leader when it has the required data state. Leader selection is therefore not fully described by the ISR list alone.

### Question

Can RecoveryGuard identify which replica was eligible for election and validate the recovered data after the controller selects an ELR candidate?

### Preconditions

- Record the Kafka version, metadata version, and ELR feature state.
- Confirm that the Admin API used by the Lab can read the ELR state for each test partition.
- Capture ISR, ELR, leader, replicas, and relevant configuration before every injected failure.
- Do not change `min.insync.replicas` during the test. Kafka documents that changing this setting can clear ELR state.
- Use `acks=all` and the external confirmed-write ledger.

### Sequence

1. Start with a healthy RF=3 partition.
2. Produce and verify confirmed writes.
3. Create a controlled replica state in which the controller reports an ELR candidate.
4. Record the ELR candidate and its fencing state.
5. Inject failures that remove the current ISR leader candidates.
6. Observe the leader selected by the controller.
7. Verify whether the new leader was selected from the ISR or from an unfenced ELR entry.
8. Compare recovered records against the external ledger.
9. Repeat with a different replica state, where no usable ELR candidate is present, if the cluster configuration supports that test.

### Expected safe result

- The test records actual ELR state. It does not infer ELR membership from replica assignment or ISR membership.
- The elected leader matches Kafka's documented eligible-leader rules for the tested feature state.
- Confirmed writes remain present.
- If no safe leader can be elected, the run is recorded as unavailable or incomplete. It must not be called a successful recovery merely because an alternate path was forced.

### Important correction

Do not use a generic forced unclean-election test as a substitute for this scenario. Kafka 4.4 ELR changes the candidate selection path. The test must discover the active feature state and actual candidate list before failure injection.

---

## RG-06 — Producer acknowledgement uncertainty during failover

**Priority:** High  
**Lab profile:** Current 3-node profile plus controlled network fault injection

### Real failure mechanism

A record can be committed by Kafka while the acknowledgement response is lost. The producer then sees a timeout or network error and cannot know from that response alone whether the record was committed.

### Question

Can the validator avoid a false data-loss verdict when the producer outcome is unknown, while still detecting a confirmed acknowledged write that is missing after recovery?

### Preconditions

- The external ledger records `SEND_INTENT` before send and the producer result after the callback.
- A network proxy or fault injector can drop the producer response path without silently changing the Kafka request path.
- Use unique record IDs and idempotent producer settings.
- The test report separates confirmed acknowledgements from timeouts and unknown outcomes.

### Sequence

1. Send a record and record `SEND_INTENT`.
2. Allow the request to reach Kafka.
3. Drop or delay the acknowledgement response.
4. Record the producer's timeout or unknown result.
5. Recover the connection and, in a separate subtest, retry according to the producer configuration.
6. Trigger or combine with a broker failover.
7. Scan the topic and compare record IDs, hashes, and duplicate counts.
8. Classify only durably recorded successful acknowledgements as confirmed writes.

### Expected safe result

- Confirmed writes are present after recovery.
- Unknown outcomes remain unknown unless the verifier finds the record.
- A missing record with only an unknown producer result is not reported as acknowledged-write loss.
- Duplicate records are measured separately and evaluated against the selected producer/application delivery contract.

This scenario is important for preventing false positive integrity failures. It tests the evidence boundary of RecoveryGuard, not only Kafka's replication behavior.

---

## 6. Additional scenarios for a later expansion

These are worth testing, but they must not delay the first controlled experiment set.

### RG-07 — Rack or availability-zone loss

Use brokers with explicit `broker.rack` labels and RF=3. Remove one simulated rack and verify replica placement and data availability. A local rack label is not proof of a real availability-zone outage. Use separate VMs or a cloud/Kubernetes environment before claiming an AZ-level test.

### RG-08 — Observation gaps and short-lived state changes

Run rapid leader/ISR changes between RecoveryGuard snapshots. Confirm that the timeline does not invent transitions. Measure the time between observations. If an important transition could have occurred between snapshots, the evidence must show that limitation.

### RG-09 — Transaction coordinator and transaction-state recovery

Use transactional producers and `read_committed` verification. Fail the transaction coordinator or a broker hosting replicas of `__transaction_state`. Compare committed, aborted, and unresolved transaction outcomes. This requires a separate application-level contract and should be designed after the non-transactional ground-truth system is stable.

## 7. Evidence required from RecoveryGuard

The current Observer collects cluster and partition metadata. It does not read Kafka records. That boundary must remain in place.

The experiments will identify whether the following evidence is available:

| Evidence | Why it matters | Current product position |
|---|---|---|
| Broker membership and controller identity | Identify broker and controller changes | Existing Observer metadata |
| Partition leader, replicas, ISR | Identify partition state changes | Existing Observer snapshot |
| ELR candidates | Explain ELR leader selection | Not confirmed in current snapshot model |
| Adding/removing replicas and reassignment state | Detect an unfinished movement | Not confirmed in current snapshot model |
| Log-directory state | Detect storage failure | Not confirmed in current snapshot model |
| KRaft quorum status and metadata lag | Separate control-plane failure from broker health | Not confirmed in current snapshot model |
| Rack/failure-domain mapping | Evaluate placement after zone loss | Requires configuration evidence |
| Replication lag and progress | Determine whether catch-up is advancing | Requires metrics or another approved evidence source |
| Record IDs and payload hashes | Prove record survival | External Lab oracle; optional customer evidence for product checks |

Do not expand the Observer or change product schemas as part of this scenario-specification task. Record every evidence gap. Stage 4 will decide which checks and evidence sources belong in the product.

## 8. Execution order

1. **RG-00:** Establish the basic failure and ledger workflow.
2. **RG-06:** Prove that confirmed and unknown producer outcomes are handled correctly.
3. **RG-01:** Add sustained workload and replica catch-up.
4. **RG-02:** Add reassignment, throttling, and a second failure.
5. **RG-05:** Validate ELR-aware recovery after the feature preflight is complete.
6. **RG-04:** Build the dedicated-controller profile and test quorum loss.
7. **RG-03:** Build the separate-storage profile, then the Strimzi/PVC extension.

RG-07 and RG-08 can run as cross-cutting tests. RG-09 is a later extension.

## 9. Acceptance criteria for Stage 3.6

Stage 3.6 is complete only when:

- The ground-truth recorder and record verifier work independently of RecoveryGuard.
- Every primary scenario has a versioned run manifest and a repeatable failure sequence.
- Every run records its Kafka version, configuration, topology, workload, fault timeline, and final state.
- Every confirmed write is checked after recovery.
- Unknown producer outcomes are not misclassified as confirmed writes.
- RecoveryGuard evidence is compared with the independent Lab result.
- Evidence gaps are documented. The test does not fill gaps with assumptions.
- Each scenario has repeated runs. Use at least 3 successful repetitions for deterministic scenarios and at least 5 for scenarios with timing-sensitive behavior, after a dry run.
- No test relies on a product verdict as its own ground truth.

## 10. Sources

1. Apache Kafka 4.4 — [Design and replication semantics](https://kafka.apache.org/44/design/design/)
2. Apache Kafka 4.4 — [Basic operations and replica reassignment](https://kafka.apache.org/44/operations/basic-kafka-operations/)
3. Apache Kafka 4.4 — [KRaft configuration and controller quorum](https://kafka.apache.org/44/operations/kraft/)
4. Apache Kafka 4.4 — [Eligible Leader Replicas](https://kafka.apache.org/44/operations/eligible-leader-replicas/)
5. Apache Kafka 4.4 — [Broker configuration, including broker.rack](https://kafka.apache.org/44/configuration/broker-configs/)
6. Apache Kafka 4.4 — [Topic configuration and min.insync.replicas](https://kafka.apache.org/44/configuration/topic-configs/)
7. Apache Kafka — [KIP-966: Eligible Leader Replicas](https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas)
8. Strimzi — [Deploying and Managing Strimzi: broker storage recovery](https://strimzi.io/docs/operators/latest/full/deploying)
9. LinkedIn Cruise Control — [Troubleshooting self-healing and replica movement](https://github.com/linkedin/cruise-control/wiki/Troubleshooting)

---

## 11. Execution status and validated baseline

### RG-00 run record: `rg-00-run-004`

One clean RG-00 run completed with a PASS result. The run used the three-broker combined KRaft profile, Kafka image `apache/kafka:4.4.0`, one partition, replication factor 3, and `min.insync.replicas=2`.

| Measurement | Result |
|---|---:|
| Confirmed writes | 10,000 |
| Preserved writes | 10,000 |
| Missing confirmed writes | 0 |
| Payload mismatches | 0 |
| Location mismatches | 0 |
| Duplicate record IDs | 0 |
| Unknown outcomes | 0 |
| Unresolved intents | 0 |
| Contradictory producer outcomes | 0 |
| Untracked records | 0 |
| Verifier status | `PASS` |

The initial leader was broker 3. The failure injector stopped broker 3. Kafka elected broker 1. Broker 3 restarted and the ISR returned to brokers 1, 2, and 3.

This is one valid run for one configuration. It does not satisfy the repeated-run acceptance requirement for the complete Stage 3.6 scenario set. Earlier runs with reused topics were contaminated by records from prior runs and are not valid data-loss evidence. Do not use them as successful or failed recovery measurements.

### Completed Lab capabilities

- The independent ground-truth recorder is implemented.
- The RG-00 active leader failure injector is implemented.
- The record verifier checks confirmed writes against the external ledger.
- Product Observer integration is exercised by the product repository workflow using a pinned Lab revision.

### Remaining experiment work

RG-01 through RG-06 and the advanced profiles remain separate work items unless a run record in this repository proves their completion. The controller-only, separate-storage, Kubernetes/Strimzi, and real availability-zone profiles are not represented by the current Compose topology.

## 12. Change and approval gates

Do not implement every scenario in one change. Before each new scenario:

1. Approve the scenario objective and acceptance criteria.
2. Confirm the Lab topology can isolate the intended fault.
3. Define the independent ground-truth events and unknown-outcome rules.
4. Identify the product evidence required and document any gaps.
5. Add a unique run manifest and repeatable failure procedure.
6. Run the required repetitions and preserve all results, including failed and inconclusive runs.

Product evidence schemas and verdict rules require separate approval in the RecoveryGuard product repository. Lab changes do not authorize product contract changes.
