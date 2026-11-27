package com.recoveryguard.lab.groundtruth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

final class RunStore {
    private static final Pattern SAFE_RUN_ID = Pattern.compile("[A-Za-z0-9._-]{1,80}");

    private final Path outputDirectory;

    RunStore(Path outputDirectory) {
        this.outputDirectory = outputDirectory.toAbsolutePath().normalize();
    }

    Run create(String requestedRunId, String topic, String bootstrapServers, String description) throws IOException {
        String runId = requestedRunId == null || requestedRunId.isBlank()
                ? UUID.randomUUID().toString()
                : requestedRunId;
        if (!SAFE_RUN_ID.matcher(runId).matches()) {
            throw new IllegalArgumentException("runId can contain only letters, digits, dot, underscore, and hyphen");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers is required");
        }

        Files.createDirectories(outputDirectory);
        Path runDirectory = outputDirectory.resolve(runId).normalize();
        if (!runDirectory.startsWith(outputDirectory)) {
            throw new IllegalArgumentException("Invalid runId path");
        }
        Files.createDirectory(runDirectory);

        ObjectNode manifest = JsonSupport.MAPPER.createObjectNode();
        manifest.put("schemaVersion", 1);
        manifest.put("runId", runId);
        manifest.put("createdAt", Instant.now().toString());
        manifest.put("topic", topic);
        manifest.put("bootstrapServers", bootstrapServers);
        manifest.put("kafkaClientVersion", "4.4.0");
        manifest.put("producerAcks", "all");
        manifest.put("producerIdempotence", true);
        manifest.put("description", description == null ? "" : description);

        Path manifestPath = runDirectory.resolve("manifest.json");
        byte[] manifestBytes = (JsonSupport.MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(manifest) + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(
                manifestPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(manifestBytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }

        EventLedger ledger = new EventLedger(runDirectory, runId);
        ledger.append("RUN_CREATED", Map.of(
                "topic", topic,
                "bootstrapServers", bootstrapServers,
                "kafkaClientVersion", "4.4.0"
        ));
        return new Run(runId, runDirectory, manifest, ledger);
    }

    Run load(String runId) throws IOException {
        if (runId == null || !SAFE_RUN_ID.matcher(runId).matches()) {
            throw new IllegalArgumentException("A valid runId is required");
        }
        Path runDirectory = outputDirectory.resolve(runId).normalize();
        if (!runDirectory.startsWith(outputDirectory)) {
            throw new IllegalArgumentException("Invalid runId path");
        }
        Path manifestPath = runDirectory.resolve("manifest.json");
        if (!Files.isRegularFile(manifestPath)) {
            throw new IOException("Run manifest not found: " + manifestPath);
        }
        JsonNode manifest = JsonSupport.MAPPER.readTree(manifestPath.toFile());
        if (!manifest.path("runId").asText().equals(runId)
                || manifest.path("schemaVersion").asInt(-1) != 1) {
            throw new IOException("Run manifest is invalid or has an unsupported schema");
        }
        return new Run(runId, runDirectory, manifest, new EventLedger(runDirectory, runId));
    }

    record Run(String runId, Path directory, JsonNode manifest, EventLedger ledger) {
        String topic() {
            return manifest.path("topic").asText();
        }

        String bootstrapServers() {
            return manifest.path("bootstrapServers").asText();
        }
    }
}
