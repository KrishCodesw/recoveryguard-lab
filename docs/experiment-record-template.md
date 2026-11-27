# Experiment Record

Use one copy of this template for each experiment run. Store the completed record with the run evidence.

## 1. Run identity

| Field | Value |
|---|---|
| Scenario ID | |
| Run ID | |
| Operator | |
| Start time (UTC) | |
| End time (UTC) | |
| Run status | NOT_STARTED / RUNNING / PASS / FAIL / INCONCLUSIVE / ABORTED |
| Repository commit | |

## 2. Kafka environment

| Field | Value |
|---|---|
| Kafka image and version | |
| Node count | |
| Process roles | |
| Controller quorum | |
| Cluster ID | |
| Host OS and Docker version | |
| Compose file revision | |

## 3. Topic configuration

| Field | Value |
|---|---|
| Topic | |
| Partition count | |
| Replication factor | |
| `min.insync.replicas` | |
| `cleanup.policy` | |
| `retention.ms` | |
| Initial leader and ISR | |

## 4. Workload

| Field | Value |
|---|---|
| Producer command | |
| Record count | |
| Payload size | |
| `acks` | |
| Idempotence | |
| Retry policy | |
| Confirmed writes | |
| Unknown outcomes | |

## 5. Failure procedure

| Field | Value |
|---|---|
| Target component | |
| Fault action | |
| Failure-start event time | |
| Replacement leader or recovery state | |
| Component restore time | |
| Failure-end event time | |
| Deviations from procedure | |

## 6. RecoveryGuard observation

| Field | Value |
|---|---|
| Product commit | |
| Observer command | |
| Capture interval and count | |
| Investigation ID | |
| Cluster ID reported by Observer | |
| Detected signals | |
| Evidence integrity result | |
| Report path | |

## 7. Ground-truth verification

| Field | Value |
|---|---|
| Verifier command | |
| Verifier status | |
| Confirmed writes | |
| Preserved writes | |
| Missing confirmed writes | |
| Payload mismatches | |
| Duplicate record IDs | |
| Unknown outcomes | |
| Unresolved intents | |
| Report path | |

## 8. Evidence files

- [ ] Run manifest
- [ ] Event ledger
- [ ] Producer output
- [ ] Verifier report
- [ ] Kafka broker logs
- [ ] Container and Docker logs
- [ ] RecoveryGuard report, if used
- [ ] Relevant configuration and command output

## 9. Result and interpretation

**Observed result:**

Write only facts supported by the retained evidence.

**Expected result:**

State the scenario acceptance criteria.

**Deviations and limitations:**

Record timing uncertainty, missing evidence, environment differences, and tool failures.

**Conclusion:**

Do not generalize the result beyond the recorded Kafka version, topology, configuration, workload, and failure procedure.

## 10. Review

| Reviewer | Review date | Decision | Notes |
|---|---|---|---|
| | | | |
