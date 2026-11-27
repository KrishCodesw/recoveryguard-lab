package com.recoveryguard.lab.groundtruth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FailureRecorderTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void recordsFailureStartAndEndAsSeparateEvents() throws Exception {
        RunStore.Run run = new RunStore(temporaryDirectory)
                .create("failure-run", "failure-topic", "localhost:29092", "");
        FailureRecorder recorder = new FailureRecorder();

        recorder.start(run, "failure-1", "kafka-2", "container-stop", "Stop follower");
        recorder.end(run, "failure-1", "completed", "Container stopped");

        List<com.fasterxml.jackson.databind.JsonNode> events = run.ledger().readAll();
        assertEquals(3, events.size());
        assertEquals("RUN_CREATED", events.get(0).path("eventType").asText());
        assertEquals("FAILURE_STARTED", events.get(1).path("eventType").asText());
        assertEquals("FAILURE_ENDED", events.get(2).path("eventType").asText());
        assertEquals("failure-1", events.get(1).path("failureId").asText());
        assertEquals("kafka-2", events.get(1).path("target").asText());
    }
}
