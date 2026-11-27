package com.recoveryguard.lab.groundtruth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
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
    private String lastEventHash = "0".repeat(64);
    private long knownFileSize = -1;

    EventLedger(Path runDirectory, String runId) {
        this.ledgerPath = runDirectory.resolve("events.jsonl").toAbsolutePath().normalize();
        this.lockPath = runDirectory.resolve("events.lock").toAbsolutePath().normalize();
        this.runId = runId;
    }

    void append(String eventType, Map<String, ?> fields) throws IOException {
        appendBatch(List.of(new Entry(eventType, fields)));
    }

    void appendBatch(List<Entry> entries) throws IOException {
        if (entries == null || entries.isEmpty()) {
            return;
        }

        FileDurability.createDirectories(ledgerPath.getParent());
        ReentrantLock localLock = JVM_LOCKS.computeIfAbsent(lockPath, ignored -> new ReentrantLock());
        localLock.lock();
        boolean lockFileExisted = Files.exists(lockPath);
        try (FileChannel lockChannel = FileChannel.open(
                     lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = lockChannel.lock()) {

            boolean ledgerFileExisted = Files.exists(ledgerPath);
            try (FileChannel ledgerChannel = FileChannel.open(
                    ledgerPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE)) {

            long size = ledgerChannel.size();
            if (knownFileSize < 0) {
                refreshAppendState(size);
            } else if (size < knownFileSize) {
                throw new IOException("Ledger was truncated outside this writer. Refuse to append.");
            } else if (size > knownFileSize) {
                // Another EventLedger instance may have appended while holding the same OS file lock.
                // Validate the full chain before accepting its new terminal hash.
                refreshAppendState(size);
            }

            if (size > 0) {
                ByteBuffer lastByte = ByteBuffer.allocate(1);
                int read = ledgerChannel.read(lastByte, size - 1);
                if (read != 1) {
                    throw new IOException("Could not inspect the final ledger byte");
                }
                lastByte.flip();
                if (lastByte.get() != (byte) '\n') {
                    throw new IOException("Ledger has an incomplete final line. Preserve it for review; do not append.");
                }
            }

            String previousHash = lastEventHash;
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            for (Entry entry : entries) {
                ObjectNode event = createEvent(entry, previousHash);
                String eventHash = Hashing.sha256(JsonSupport.MAPPER.writeValueAsBytes(event));
                event.put("eventHash", eventHash);
                previousHash = eventHash;
                output.write((JsonSupport.MAPPER.writeValueAsString(event) + "\n")
                        .getBytes(StandardCharsets.UTF_8));
            }

            byte[] batchBytes = output.toByteArray();
            ledgerChannel.position(size);
            ByteBuffer record = ByteBuffer.wrap(batchBytes);
            while (record.hasRemaining()) {
                ledgerChannel.write(record);
            }
            ledgerChannel.force(true);
            knownFileSize = size + batchBytes.length;
            lastEventHash = previousHash;

            if (!ledgerFileExisted || !lockFileExisted) {
                FileDurability.forceDirectory(ledgerPath.getParent());
            }
            }
        } finally {
            localLock.unlock();
        }
    }

    private void refreshAppendState(long currentSize) throws IOException {
        List<JsonNode> existingEvents = readAll();
        lastEventHash = existingEvents.isEmpty()
                ? "0".repeat(64)
                : existingEvents.get(existingEvents.size() - 1).path("eventHash").asText();
        knownFileSize = currentSize;
    }

    private ObjectNode createEvent(Entry entry, String previousHash) {
        if (entry.eventType() == null || entry.eventType().isBlank()) {
            throw new IllegalArgumentException("eventType is required");
        }

        ObjectNode event = JsonSupport.MAPPER.createObjectNode();
        event.put("schemaVersion", SCHEMA_VERSION);
        event.put("runId", runId);
        event.put("eventId", UUID.randomUUID().toString());
        event.put("eventType", entry.eventType());
        event.put("recordedAt", Instant.now().toString());
        event.put("previousEventHash", previousHash);

        for (Map.Entry<String, ?> field : entry.fields().entrySet()) {
            String name = field.getKey();
            if (name == null || name.isBlank()
                    || name.equals("schemaVersion")
                    || name.equals("runId")
                    || name.equals("eventId")
                    || name.equals("eventType")
                    || name.equals("recordedAt")
                    || name.equals("previousEventHash")
                    || name.equals("eventHash")) {
                throw new IllegalArgumentException("Invalid or reserved event field: " + name);
            }
            if (field.getValue() != null) {
                event.set(name, JsonSupport.MAPPER.valueToTree(field.getValue()));
            }
        }
        return event;
    }

    List<JsonNode> readAll() throws IOException {
        List<JsonNode> events = new ArrayList<>();
        if (!Files.exists(ledgerPath)) {
            throw new IOException("Ledger does not exist: " + ledgerPath);
        }
        long fileSize = Files.size(ledgerPath);
        if (fileSize == 0) {
            return List.of();
        }
        try (FileChannel channel = FileChannel.open(ledgerPath, StandardOpenOption.READ)) {
            ByteBuffer lastByte = ByteBuffer.allocate(1);
            int read = channel.read(lastByte, fileSize - 1);
            if (read != 1) {
                throw new IOException("Could not inspect the final ledger byte");
            }
            lastByte.flip();
            if (lastByte.get() != (byte) '\n') {
                throw new IOException("Ledger has an incomplete final line");
            }
        }

        String expectedPreviousHash = "0".repeat(64);
        java.util.Set<String> eventIds = new java.util.HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(ledgerPath, StandardCharsets.UTF_8)) {
            String line;
            long lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    throw new IOException("Blank ledger line at line " + lineNumber);
                }
                JsonNode parsed;
                try {
                    parsed = JsonSupport.MAPPER.readTree(line);
                } catch (Exception exception) {
                    throw new IOException("Invalid JSON at ledger line " + lineNumber, exception);
                }
                if (!parsed.isObject()) {
                    throw new IOException("Ledger event is not a JSON object at line " + lineNumber);
                }
                if (!parsed.path("runId").asText().equals(runId)) {
                    throw new IOException("Ledger runId mismatch at line " + lineNumber);
                }
                if (parsed.path("schemaVersion").asInt(-1) != SCHEMA_VERSION) {
                    throw new IOException("Unsupported ledger schema at line " + lineNumber);
                }
                String eventId = parsed.path("eventId").asText();
                String eventHash = parsed.path("eventHash").asText();
                if (eventId.isBlank() || parsed.path("eventType").asText().isBlank()
                        || parsed.path("recordedAt").asText().isBlank() || eventHash.isBlank()) {
                    throw new IOException("Required ledger field missing at line " + lineNumber);
                }
                if (!eventIds.add(eventId)) {
                    throw new IOException("Duplicate eventId at ledger line " + lineNumber);
                }
                if (!parsed.path("previousEventHash").asText().equals(expectedPreviousHash)) {
                    throw new IOException("Ledger hash chain mismatch at line " + lineNumber);
                }

                ObjectNode eventWithoutHash = ((ObjectNode) parsed).deepCopy();
                eventWithoutHash.remove("eventHash");
                String computedHash = Hashing.sha256(JsonSupport.MAPPER.writeValueAsBytes(eventWithoutHash));
                if (!computedHash.equals(eventHash)) {
                    throw new IOException("Ledger event hash mismatch at line " + lineNumber);
                }
                expectedPreviousHash = eventHash;
                events.add(parsed);
            }
        }
        return List.copyOf(events);
    }

    Path path() {
        return ledgerPath;
    }

    record Entry(String eventType, Map<String, ?> fields) {
        Entry {
            if (fields == null) {
                fields = Map.of();
            } else {
                fields = Map.copyOf(fields);
            }
        }
    }
}
