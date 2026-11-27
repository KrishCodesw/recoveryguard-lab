package com.recoveryguard.lab.groundtruth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

final class RecordVerifier {
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);

    VerificationResult verify(RunStore.Run run, Path reportPath, long timeoutMs) throws Exception {
        if (timeoutMs < 1_000 || timeoutMs > 900_000) {
            throw new IllegalArgumentException("timeoutMs must be between 1000 and 900000");
        }

        List<JsonNode> events = run.ledger().readAll();
        Map<String, String> intentHashes = new HashMap<>();
        Map<String, JsonNode> confirmedAcks = new HashMap<>();
        Set<String> unknownOutcomes = new HashSet<>();
        Set<String> failedOutcomes = new HashSet<>();
        int contradictoryOutcomes = 0;

        for (JsonNode event : events) {
            String id = event.path("recordId").asText("");
            switch (event.path("eventType").asText()) {
                case "SEND_INTENT" -> {
                    if (id.isBlank()) {
                        throw new IOException("SEND_INTENT has no recordId");
                    }
                    String hash = event.path("payloadSha256").asText("");
                    if (!hash.matches("[0-9a-f]{64}")) {
                        throw new IOException("SEND_INTENT has an invalid payload hash: " + id);
                    }
                    if (intentHashes.putIfAbsent(id, hash) != null) {
                        throw new IOException("Duplicate SEND_INTENT for recordId " + id);
                    }
                }
                case "ACK_CONFIRMED" -> {
                    if (id.isBlank()) {
                        throw new IOException("ACK_CONFIRMED has no recordId");
                    }
                    if (confirmedAcks.putIfAbsent(id, event) != null) {
                        contradictoryOutcomes++;
                    }
                }
                case "OUTCOME_UNKNOWN" -> {
                    if (id.isBlank()) {
                        throw new IOException("OUTCOME_UNKNOWN has no recordId");
                    }
                    unknownOutcomes.add(id);
                }
                case "SEND_ERROR" -> {
                    if (id.isBlank()) {
                        throw new IOException("SEND_ERROR has no recordId");
                    }
                    failedOutcomes.add(id);
                }
                default -> {
                    // Run, producer batch, and failure-injector events are not record outcomes.
                }
            }
        }

        for (Map.Entry<String, JsonNode> ackEntry : confirmedAcks.entrySet()) {
            String id = ackEntry.getKey();
            JsonNode ack = ackEntry.getValue();
            if (!intentHashes.containsKey(id)) {
                throw new IOException("ACK_CONFIRMED has no matching SEND_INTENT: " + id);
            }
            if (!intentHashes.get(id).equals(ack.path("payloadSha256").asText())) {
                throw new IOException("ACK_CONFIRMED payload hash does not match SEND_INTENT: " + id);
            }
            if (!run.topic().equals(ack.path("topic").asText())) {
                throw new IOException("ACK_CONFIRMED topic does not match run topic: " + id);
            }
            if (ack.path("partition").asInt(-1) < 0 || ack.path("offset").asLong(-1) < 0) {
                throw new IOException("ACK_CONFIRMED has invalid partition or offset: " + id);
            }
        }
        for (String id : unknownOutcomes) {
            if (!intentHashes.containsKey(id)) {
                throw new IOException("OUTCOME_UNKNOWN has no matching SEND_INTENT: " + id);
            }
        }
        for (String id : failedOutcomes) {
            if (!intentHashes.containsKey(id)) {
                throw new IOException("SEND_ERROR has no matching SEND_INTENT: " + id);
            }
        }

        Map<String, List<ObservedRecord>> observedById = scanTopic(run, timeoutMs);
        boolean scanComplete = observedById != null;
        if (!scanComplete) {
            observedById = Map.of();
        }

        int confirmedCount = confirmedAcks.size();
        int preservedCount = 0;
        int missingCount = 0;
        int payloadMismatchCount = 0;
        int locationMismatchCount = 0;
        int duplicateRecordIdCount = 0;
        int unknownFoundCount = 0;
        int unknownMissingCount = 0;
        int unresolvedIntentFoundCount = 0;
        int unresolvedIntentMissingCount = 0;
        int untrackedRecordCount = 0;

        Set<String> allLedgerIds = new HashSet<>(intentHashes.keySet());
        for (List<ObservedRecord> records : observedById.values()) {
            if (records.size() > 1) {
                duplicateRecordIdCount++;
            }
        }

        for (Map.Entry<String, JsonNode> ackEntry : confirmedAcks.entrySet()) {
            String id = ackEntry.getKey();
            JsonNode expected = ackEntry.getValue();
            List<ObservedRecord> actual = observedById.getOrDefault(id, List.of());
            if (actual.isEmpty()) {
                missingCount++;
                continue;
            }

            String expectedHash = expected.path("payloadSha256").asText("");
            int expectedPartition = expected.path("partition").asInt(-1);
            long expectedOffset = expected.path("offset").asLong(-1);
            boolean matchingPayload = actual.stream()
                    .anyMatch(record -> record.payloadSha256().equals(expectedHash));
            if (!matchingPayload) {
                payloadMismatchCount++;
                continue;
            }
            boolean matchingLocation = actual.stream().anyMatch(record ->
                    record.payloadSha256().equals(expectedHash)
                            && record.partition() == expectedPartition
                            && record.offset() == expectedOffset);
            if (!matchingLocation) {
                locationMismatchCount++;
                continue;
            }
            preservedCount++;
        }

        for (String id : unknownOutcomes) {
            if (confirmedAcks.containsKey(id)) {
                contradictoryOutcomes++;
                continue;
            }
            List<ObservedRecord> actual = observedById.getOrDefault(id, List.of());
            if (actual.isEmpty()) {
                unknownMissingCount++;
            } else if (actual.stream().noneMatch(
                    record -> record.payloadSha256().equals(intentHashes.get(id)))) {
                payloadMismatchCount++;
            } else {
                unknownFoundCount++;
            }
        }

        for (String id : intentHashes.keySet()) {
            if (!confirmedAcks.containsKey(id)
                    && !unknownOutcomes.contains(id)
                    && !failedOutcomes.contains(id)) {
                List<ObservedRecord> actual = observedById.getOrDefault(id, List.of());
                if (actual.isEmpty()) {
                    unresolvedIntentMissingCount++;
                } else if (actual.stream().noneMatch(
                        record -> record.payloadSha256().equals(intentHashes.get(id)))) {
                    payloadMismatchCount++;
                } else {
                    unresolvedIntentFoundCount++;
                }
            }
        }

        for (Map.Entry<String, List<ObservedRecord>> entry : observedById.entrySet()) {
            if (!allLedgerIds.contains(entry.getKey())) {
                untrackedRecordCount += entry.getValue().size();
            }
        }

        String status;
        if (!scanComplete) {
            status = "INCOMPLETE";
        } else if (missingCount > 0 || payloadMismatchCount > 0
                || locationMismatchCount > 0 || duplicateRecordIdCount > 0
                || contradictoryOutcomes > 0 || untrackedRecordCount > 0) {
            status = "INTEGRITY_FAILURE";
        } else if (unknownFoundCount > 0 || unknownMissingCount > 0
                || unresolvedIntentFoundCount > 0 || unresolvedIntentMissingCount > 0) {
            status = "INCONCLUSIVE";
        } else if (confirmedCount == 0) {
            status = "INCOMPLETE";
        } else {
            status = "PASS";
        }

        ObjectNode report = JsonSupport.MAPPER.createObjectNode();
        report.put("schemaVersion", 1);
        report.put("runId", run.runId());
        report.put("topic", run.topic());
        report.put("verifiedAt", Instant.now().toString());
        report.put("status", status);
        report.put("scanComplete", scanComplete);
        report.put("confirmedWrites", confirmedCount);
        report.put("preservedWrites", scanComplete ? preservedCount : 0);
        report.put("missingConfirmedWrites", scanComplete ? missingCount : 0);
        report.put("payloadMismatches", scanComplete ? payloadMismatchCount : 0);
        report.put("locationMismatches", scanComplete ? locationMismatchCount : 0);
        report.put("duplicateRecordIds", scanComplete ? duplicateRecordIdCount : 0);
        report.put("unknownOutcomesFound", scanComplete ? unknownFoundCount : 0);
        report.put("unknownOutcomesMissing", scanComplete ? unknownMissingCount : 0);
        report.put("unresolvedIntentsFound", scanComplete ? unresolvedIntentFoundCount : 0);
        report.put("unresolvedIntentsMissing", scanComplete ? unresolvedIntentMissingCount : 0);
        report.put("contradictoryProducerOutcomes", contradictoryOutcomes);
        report.put("untrackedRecords", scanComplete ? untrackedRecordCount : 0);
        report.put("consumerGroupJoined", false);
        report.put("offsetsCommitted", false);
        report.put("isolationLevel", "read_uncommitted");

        if (reportPath != null) {
            Path absoluteReportPath = reportPath.toAbsolutePath().normalize();
            if (absoluteReportPath.startsWith(run.directory())) {
                throw new IllegalArgumentException("Write the verification report outside the run directory");
            }
            Path parent = absoluteReportPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(
                    absoluteReportPath,
                    JsonSupport.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report)
                            + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE
            );
        }

        return new VerificationResult(status, report);
    }

    private Map<String, List<ObservedRecord>> scanTopic(RunStore.Run run, long timeoutMs) throws Exception {
        Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, run.bootstrapServers());

        List<TopicPartition> partitions = new ArrayList<>();
        try (AdminClient admin = AdminClient.create(adminProperties)) {
            Map<String, TopicDescription> descriptions = admin.describeTopics(List.of(run.topic()))
                    .allTopicNames().get(timeoutMs, TimeUnit.MILLISECONDS);
            TopicDescription description = descriptions.get(run.topic());
            if (description == null || description.partitions().isEmpty()) {
                throw new IOException("Topic does not exist or has no partitions: " + run.topic());
            }
            description.partitions().forEach(info ->
                    partitions.add(new TopicPartition(run.topic(), info.partition())));
        }

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, run.bootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "recoveryguard-lab-verifier-" + run.runId());
        consumerProperties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        consumerProperties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");

        Map<String, List<ObservedRecord>> recordsById = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.assign(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions, Duration.ofSeconds(30));
            consumer.seekToBeginning(partitions);

            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            boolean complete = false;
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_INTERVAL);
                for (ConsumerRecord<String, String> record : records) {
                    if (record.key() == null) {
                        continue;
                    }
                    String payloadHash = record.value() == null
                            ? Hashing.sha256(new byte[0])
                            : Hashing.sha256(record.value().getBytes(StandardCharsets.UTF_8));
                    recordsById.computeIfAbsent(record.key(), ignored -> new ArrayList<>())
                            .add(new ObservedRecord(
                                    record.topic(), record.partition(), record.offset(), payloadHash));
                }

                complete = true;
                for (TopicPartition partition : partitions) {
                    if (consumer.position(partition) < endOffsets.getOrDefault(partition, 0L)) {
                        complete = false;
                        break;
                    }
                }
                if (complete) {
                    break;
                }
            }
            if (!complete) {
                return null;
            }
        }
        return recordsById;
    }

    record VerificationResult(String status, ObjectNode report) {
    }

    private record ObservedRecord(String topic, int partition, long offset, String payloadSha256) {
    }
}
