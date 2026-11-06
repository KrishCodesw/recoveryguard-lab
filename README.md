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

Stop the containers and keep their data:

```bash
docker compose down
```

Stop the containers and remove the test data:

```bash
docker compose down --volumes --remove-orphans
```

## Lab boundaries

- Lab code and ground truth must stay independent of RecoveryGuard product implementation.
- The Lab must use only an approved, pinned verdict contract when a contract is introduced.
- Do not add product classes or product Maven dependencies to this repository.
- The cluster uses plaintext communication. Do not use it for production data or expose its ports to an untrusted network.
- The fixed cluster ID and named volumes are for this disposable test cluster. Remove the volumes when you need a clean cluster.
