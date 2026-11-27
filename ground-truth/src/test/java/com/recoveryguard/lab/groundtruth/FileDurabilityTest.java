package com.recoveryguard.lab.groundtruth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class FileDurabilityTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void createsAndSynchronizesNestedDirectories() throws Exception {
        Path nested = temporaryDirectory.resolve("runs").resolve("run-1");

        FileDurability.createDirectories(nested);
        FileDurability.forceDirectory(nested);

        assertTrue(Files.isDirectory(nested));
    }
}
