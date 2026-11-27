package com.recoveryguard.lab.groundtruth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GroundTruthCliTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void rejectsUnknownOptionsBeforeLoadingRun() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> GroundTruthCli.execute(new String[]{
                        "produce",
                        "--run-id", "missing-run",
                        "--count", "10",
                        "--interval-mss", "1000"
                }));

        assertTrue(exception.getMessage().contains("--interval-mss"));
    }

    @Test
    void rejectsUnknownCommandOptionsBeforeOpeningKafka() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> GroundTruthCli.execute(new String[]{
                        "failure-start",
                        "--run-id", "missing-run",
                        "--failure-id", "failure-1",
                        "--target", "kafka-1",
                        "--action", "stop",
                        "--bootstrp-servers", "localhost:29092"
                }));

        assertTrue(exception.getMessage().contains("--bootstrp-servers"));
    }

    @Test
    void defaultVerificationReportNamesAreUnique() throws Exception {
        RunStore.Run run = new RunStore(temporaryDirectory)
                .create("report-run", "report-topic", "localhost:29092", "");

        Path first = GroundTruthCli.defaultReportPath(run);
        Path second = GroundTruthCli.defaultReportPath(run);

        assertNotEquals(first, second);
        assertTrue(first.toString().contains("report-run-verification-"));
        assertTrue(second.toString().endsWith(".json"));
    }
}
