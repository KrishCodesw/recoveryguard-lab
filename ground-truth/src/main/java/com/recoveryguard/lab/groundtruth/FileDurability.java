package com.recoveryguard.lab.groundtruth;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class FileDurability {
    private FileDurability() {
    }

    static void createDirectories(Path directory) throws IOException {
        Path absolute = directory.toAbsolutePath().normalize();
        List<Path> missing = new ArrayList<>();

        for (Path current = absolute; current != null && !Files.exists(current); current = current.getParent()) {
            missing.add(current);
        }

        Files.createDirectories(absolute);

        // Persist each new directory entry in its parent, from the highest new directory downward.
        Collections.reverse(missing);
        for (Path createdDirectory : missing) {
            Path parent = createdDirectory.getParent();
            if (parent != null) {
                forceDirectory(parent);
            }
        }
    }

    static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (UnsupportedOperationException exception) {
            throw new IOException("The filesystem does not support directory synchronization: " + directory,
                    exception);
        }
    }
}
