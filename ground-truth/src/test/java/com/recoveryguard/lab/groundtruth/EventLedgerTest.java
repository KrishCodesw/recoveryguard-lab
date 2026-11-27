package com.recoveryguard.lab.groundtruth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLedgerTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void appendsBatchAsDurableJsonLines() throws Exception {
        EventLedger ledger = new EventLedger(temporaryDirectory.resolve("run-1"), "run-1");

        ledger.appendBatch(List.of(
                new EventLedger.Entry("SEND_INTENT", Map.of(
                        "recordId", "record-1",
                        "payloadSha256", "abc123"
                )),
                new EventLedger.Entry("ACK_CONFIRMED", Map.of(
                        "recordId", "record-1",
                        "partition", 1,
                        "offset", 42L
                ))
        ));

        List<com.fasterxml.jackson.databind.JsonNode> events = ledger.readAll();
        assertEquals(2, events.size());
        assertEquals("SEND_INTENT", events.get(0).path("eventType").asText());
        assertEquals("ACK_CONFIRMED", events.get(1).path("eventType").asText());
        assertEquals("run-1", events.get(0).path("runId").asText());
        assertTrue(Files.readString(ledger.path()).endsWith("\n"));
    }

    @Test
    void refusesToAppendAfterAnIncompleteFinalLine() throws Exception {
        Path runDirectory = temporaryDirectory.resolve("run-2");
        Files.createDirectories(runDirectory);
        Files.writeString(runDirectory.resolve("events.jsonl"), "{\"partial\":true}");

        EventLedger ledger = new EventLedger(runDirectory, "run-2");
        assertThrows(IOException.class,
                () -> ledger.append("RUN_CREATED", Map.of("topic", "test-topic")));
    }
    @Test
    void detectsChangesToExistingLedgerEvents() throws Exception {
        EventLedger ledger = new EventLedger(temporaryDirectory.resolve("run-3"), "run-3");
        ledger.append("SEND_INTENT", Map.of(
                "recordId", "record-original",
                "payloadSha256", "abc123"
        ));

        String changed = Files.readString(ledger.path()).replace("record-original", "record-edited");
        Files.writeString(ledger.path(), changed);

        assertThrows(IOException.class, ledger::readAll);
    }

    @Test
    void acceptsValidAppendsFromAnotherLedgerInstance() throws Exception {
        Path runDirectory = temporaryDirectory.resolve("shared-run");
        EventLedger producerLedger = new EventLedger(runDirectory, "shared-run");
        EventLedger failureRecorderLedger = new EventLedger(runDirectory, "shared-run");

        producerLedger.append("RUN_CREATED", Map.of("topic", "shared-topic"));
        failureRecorderLedger.append("FAILURE_STARTED", Map.of(
                "failureId", "broker-2-stop",
                "target", "kafka-2"
        ));
        producerLedger.append("SEND_INTENT", Map.of(
                "recordId", "record-1",
                "payloadSha256", "abc123"
        ));

        List<com.fasterxml.jackson.databind.JsonNode> events = producerLedger.readAll();
        assertEquals(3, events.size());
        assertEquals("RUN_CREATED", events.get(0).path("eventType").asText());
        assertEquals("FAILURE_STARTED", events.get(1).path("eventType").asText());
        assertEquals("SEND_INTENT", events.get(2).path("eventType").asText());
    }

}
