package com.recoveryguard.lab.groundtruth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunStoreTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void createsAndLoadsRunManifest() throws Exception {
        RunStore store = new RunStore(temporaryDirectory);
        RunStore.Run created = store.create(
                "run-2026-01",
                "recoveryguard-run-2026-01",
                "localhost:29092",
                "Basic leader failover"
        );

        RunStore.Run loaded = store.load(created.runId());
        assertEquals(created.runId(), loaded.runId());
        assertEquals("recoveryguard-run-2026-01", loaded.topic());
        assertEquals("localhost:29092", loaded.bootstrapServers());
        assertEquals(1, loaded.ledger().readAll().size());
        assertTrue(loaded.directory().resolve("manifest.json").toFile().isFile());
    }

    @Test
    void doesNotOverwriteAnExistingRun() throws Exception {
        RunStore store = new RunStore(temporaryDirectory);
        store.create("fixed-run", "topic-a", "localhost:29092", "");

        assertThrows(java.nio.file.FileAlreadyExistsException.class,
                () -> store.create("fixed-run", "topic-b", "localhost:29092", ""));
    }

    @Test
    void rejectsPathTraversalRunIds() {
        RunStore store = new RunStore(temporaryDirectory);
        assertThrows(IllegalArgumentException.class,
                () -> store.load("../outside"));
    }
}
