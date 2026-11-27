package com.recoveryguard.lab.groundtruth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

final class EventLedger {
    private static final int SCHEMA_VERSION = 1;
    private static final Map<Path, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();

    private final Path ledgerPath;
    private final Path lockPath;
    private final String runId;

    EventLedger(Path runDirectory, String runId) {
        this.ledgerPath = runDirectory.resolve("events.jsonl").toAbsolutePath().normalize();
        this.lockPath = runDirectory.resolve("events.lock").toAbsolutePath().normalize();
        this.runId = runId;
    }

    void append(String eventType, Map<String, ?> fields) throws IOException {
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType is required");
        }

        ObjectNode event = JsonSupport.MAPPER.createObjectNode();
        event.put("schemaVersion", SCHEMA_VERSION);
        event.put("runId", runId);
        event.put("eventId", UUID.randomUUID().toString());
        event.put("eventType", eventType);
        event.put("recordedAt", Instant.now().toString());

        for (Map.Entry<String, ?> field : fields.entrySet()) {
            String name = field.getKey();
            if (name == null || name.isBlank()
                    || name.equals("schemaVersion")
                    || name.equals("runId")
                    || name.equals("eventId")
                    || name.equals("eventType")
                    || name.equals("recordedAt")) {
                throw new IllegalArgumentException("Invalid or reserved event field: " + name);
            }
            if (field.getValue() != null) {
                event.set(name, JsonSupport.MAPPER.valueToTree(field.getValue()));
            }
        }

        byte[] bytes = (JsonSupport.MAPPER.writeValueAsString(event) + "\n")
                .getBytes(StandardCharsets.UTF_8);

        Files.createDirectories(ledgerPath.getParent());
        ReentrantLock localLock = JVM_LOCKS.computeIfAbsent(lockPath, ignored -> new ReentrantLock());
        localLock.lock();
        try (FileChannel lockChannel = FileChannel.open(
                     lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = lockChannel.lock();
             FileChannel ledgerChannel = FileChannel.open(
                     ledgerPath,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE)) {

            long size = ledgerChannel.size();
            if (size > 0) {
                ByteBuffer lastByte = ByteBuffer.allocate(1);
                ledgerChannel.read(lastByte, size - 1);
                lastByte.flip();
                if (lastByte.get() != (byte) '\n') {
                    throw new IOException("Ledger has an incomplete final line. Preserve it for review; do not append.");
                }
            }

            ledgerChannel.position(size);
            ByteBuffer record = ByteBuffer.wrap(bytes);
            while (record.hasRemaining()) {
                ledgerChannel.write(record);
            }
            ledgerChannel.force(true);
        } finally {
            localLock.unlock();
        }
    }

    List<JsonNode> readAll() throws IOException {
        List<JsonNode> events = new ArrayList<>();
        if (!Files.exists(ledgerPath)) {
            throw new IOException("Ledger does not exist: " + ledgerPath);
        }

        try (BufferedReader reader = Files.newBufferedReader(ledgerPath, StandardCharsets.UTF_8)) {
            String line;
            long lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    throw new IOException("Blank ledger line at line " + lineNumber);
                }
                JsonNode event;
                try {
                    event = JsonSupport.MAPPER.readTree(line);
                } catch (Exception exception) {
                    throw new IOException("Invalid JSON at ledger line " + lineNumber, exception);
                }
                if (!event.path("runId").asText().equals(runId)) {
                    throw new IOException("Ledger runId mismatch at line " + lineNumber);
                }
                if (event.path("schemaVersion").asInt(-1) != SCHEMA_VERSION) {
                    throw new IOException("Unsupported ledger schema at line " + lineNumber);
                }
                if (event.path("eventId").asText().isBlank()
                        || event.path("eventType").asText().isBlank()
                        || event.path("recordedAt").asText().isBlank()) {
                    throw new IOException("Required ledger field missing at line " + lineNumber);
                }
                events.add(event);
            }
        }
        return List.copyOf(events);
    }

    Path path() {
        return ledgerPath;
    }
}
