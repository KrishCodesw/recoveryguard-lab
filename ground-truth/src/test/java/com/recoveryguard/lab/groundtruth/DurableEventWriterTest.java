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
        assertEquals(0, events.get(0).path("sequence").asInt(-1));
        assertEquals(599, events.get(599).path("sequence").asInt(-1));
    }
}
