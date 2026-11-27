package com.recoveryguard.lab.groundtruth;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

final class FailureRecorder {
    void start(RunStore.Run run, String failureId, String target, String action, String details)
            throws IOException {
        validate(failureId, "failureId");
        validate(target, "target");
        validate(action, "action");
        run.ledger().append("FAILURE_STARTED", Map.of(
                "failureId", failureId,
                "target", target,
                "action", action,
                "details", details == null ? "" : details,
                "startedAt", Instant.now().toString()
        ));
    }

    void end(RunStore.Run run, String failureId, String result, String details)
            throws IOException {
        validate(failureId, "failureId");
        validate(result, "result");
        run.ledger().append("FAILURE_ENDED", Map.of(
                "failureId", failureId,
                "result", result,
                "details", details == null ? "" : details,
                "endedAt", Instant.now().toString()
        ));
    }

    private static void validate(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
    }
}
