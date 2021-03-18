package net.sansa_stack.integration.test;

import com.google.common.collect.Streams;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;


public class IOUtils {

    /** Find the newest file matching a given pattern. Raise a {@link java.util.NoSuchElementException}
     * if there is no matching item.
     */
    public static Path findLatestFile(Path basePath, String pattern) throws IOException, NoSuchElementException {
        Path result;
        try (DirectoryStream<Path> dirStream =
                     Files.newDirectoryStream(basePath, pattern)) {
            result = Streams.stream(dirStream.iterator())
                    .sorted((a, b) -> {
                        try {
                            return Files.getLastModifiedTime(a).compareTo(Files.getLastModifiedTime(b));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .findFirst()
                    .orElseThrow(() -> new NoSuchElementException("No suitable jar bundle found at: " + basePath));
        }

        return result;
    }

}
