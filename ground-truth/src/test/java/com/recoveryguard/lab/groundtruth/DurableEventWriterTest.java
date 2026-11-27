package com.recoveryguard.lab.groundtruth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void appendRacingCloseDoesNotDropAcceptedEvents() throws Exception {
        EventLedger ledger = new EventLedger(temporaryDirectory.resolve("race-run"), "race-run");
        DurableEventWriter writer = new DurableEventWriter(ledger);
        CountDownLatch firstAppendAccepted = new CountDownLatch(1);
        AtomicInteger nextSequence = new AtomicInteger();
        ConcurrentLinkedQueue<Integer> accepted = new ConcurrentLinkedQueue<>();

        Thread appender = new Thread(() -> {
            for (int attempt = 0; attempt < 20000; attempt++) {
                int sequence = nextSequence.getAndIncrement();
                try {
                    writer.append("RACE_EVENT", Map.of("sequence", sequence));
                    accepted.add(sequence);
                    firstAppendAccepted.countDown();
                } catch (IOException closedOrFailed) {
                    break;
                }
            }
        }, "writer-race-appender");

        appender.start();
        assertTrue(firstAppendAccepted.await(5, TimeUnit.SECONDS));
        writer.close();
        appender.join(5000);
        assertTrue(!appender.isAlive(), "Appender did not stop after close");

        Set<Integer> expected = Set.copyOf(accepted);
        Set<Integer> actual = ledger.readAll().stream()
                .filter(event -> event.path("eventType").asText().equals("RACE_EVENT"))
                .map(event -> event.path("sequence").asInt(-1))
                .collect(Collectors.toSet());
        assertEquals(expected, actual);
    }

    @Test
    void interruptedCloseCanBeRetried() throws Exception {
        EventLedger ledger = new EventLedger(temporaryDirectory.resolve("retry-run"), "retry-run");
        DurableEventWriter writer = new DurableEventWriter(ledger);
        writer.append("TEST_EVENT", Map.of("sequence", 1));

        Thread.currentThread().interrupt();
        assertThrows(IOException.class, writer::close);
        assertTrue(Thread.interrupted(), "Test must clear the interrupt flag before retrying");

        writer.close();
        assertEquals(1, ledger.readAll().size());
    }

    @Test
    void rejectsAppendAfterClose() throws Exception {
        EventLedger ledger = new EventLedger(temporaryDirectory.resolve("closed-run"), "closed-run");
        DurableEventWriter writer = new DurableEventWriter(ledger);
        writer.append("BEFORE_CLOSE", Map.of("sequence", 0));
        writer.close();

        assertThrows(IOException.class,
                () -> writer.append("TOO_LATE", Map.of("sequence", 1)));
        assertEquals(1, ledger.readAll().size());
    }
}
