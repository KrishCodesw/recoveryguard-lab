package com.recoveryguard.lab.groundtruth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DurableEventWriterTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void flushesQueuedEventsBeforeCloseReturns() throws Exception {
        EventLedger ledger = new EventLedger(temporaryDirectory.resolve("writer-run"), "writer-run");

        try (DurableEventWriter writer = new DurableEventWriter(ledger)) {
            for (int index = 0; index < 600; index++) {
                writer.append("TEST_EVENT", Map.of("sequence", index));
            }
        }

        List<com.fasterxml.jackson.databind.JsonNode> events = ledger.readAll();
        assertEquals(600, events.size());
        for (int index = 0; index < events.size(); index++) {
            assertEquals(index, events.get(index).path("sequence").asInt(-1),
                    "Unexpected event sequence at index " + index);
        }
    }
}
