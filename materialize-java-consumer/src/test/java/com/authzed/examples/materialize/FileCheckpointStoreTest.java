package com.authzed.examples.materialize;

import static org.junit.jupiter.api.Assertions.*;

import com.authzed.api.v1.ZedToken;
import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FileCheckpointStoreTest {
    private static final String IDENTITY = "materialize-instance-A|destination-A";

    @TempDir Path directory;

    @Test
    void missingCheckpointHasNoResumeToken() throws Exception {
        try (var store = new FileCheckpointStore(checkpoint(), IDENTITY)) {
            assertNull(store.load());
        }
    }

    @Test
    void latestSavedOpaqueTokenSurvivesClosingAndReopening() throws Exception {
        // Tokens are opaque: the store must preserve them, not compare revision strings.
        var latest = token("opaque-token-2+/=");
        try (var store = new FileCheckpointStore(checkpoint(), IDENTITY)) {
            store.save(token("opaque-token-9+/="));
            store.save(latest);
            assertEquals(latest, store.load());
        }
        try (var reopened = new FileCheckpointStore(checkpoint(), IDENTITY)) {
            assertEquals(latest, reopened.load());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"materialize-instance-B|destination-A", "materialize-instance-A|destination-B"})
    void refusesCheckpointForDifferentSourceOrDestination(String otherIdentity) throws Exception {
        var completed = token("durable-completed-revision");
        try (var store = new FileCheckpointStore(checkpoint(), IDENTITY)) {
            store.save(completed);
        }
        byte[] before = Files.readAllBytes(checkpoint());
        assertThrows(IOException.class, () -> {
            try (var wrong = new FileCheckpointStore(checkpoint(), otherIdentity)) {
                wrong.load();
            }
        });
        assertArrayEquals(before, Files.readAllBytes(checkpoint()), "rejected ownership must not modify the checkpoint");
        // A failed opener must also release any lock it acquired.
        try (var original = new FileCheckpointStore(checkpoint(), IDENTITY)) {
            assertEquals(completed, original.load());
        }
    }

    @Test
    void excludesSecondConsumerUntilFirstReleasesItsLock() throws Exception {
        try (var owner = new FileCheckpointStore(checkpoint(), IDENTITY)) {
            owner.save(token("first-owner"));
            Exception failure = assertThrows(Exception.class, () -> {
                try (var unexpected = new FileCheckpointStore(checkpoint(), IDENTITY)) {
                    fail("a second consumer acquired the same checkpoint lock");
                }
            });
            assertTrue(failure instanceof IOException || failure instanceof OverlappingFileLockException,
                    "lock acquisition should fail explicitly: " + failure);
            assertEquals(token("first-owner"), owner.load());
        }
        try (var successor = new FileCheckpointStore(checkpoint(), IDENTITY)) {
            assertEquals(token("first-owner"), successor.load());
            successor.save(token("second-owner"));
        }
        try (var reopened = new FileCheckpointStore(checkpoint(), IDENTITY)) {
            assertEquals(token("second-owner"), reopened.load());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t", "bad\nrevision", "bad\rrevision"})
    void invalidTokenCannotCorruptPreviouslyDurableCheckpoint(String invalid) throws Exception {
        var completed = token("last-safe-completed-revision");
        try (var store = new FileCheckpointStore(checkpoint(), IDENTITY)) {
            store.save(completed);
            byte[] before = Files.readAllBytes(checkpoint());
            assertThrows(IOException.class, () -> store.save(token(invalid)));
            assertArrayEquals(before, Files.readAllBytes(checkpoint()));
            assertEquals(completed, store.load());
        }
        try (var reopened = new FileCheckpointStore(checkpoint(), IDENTITY)) {
            assertEquals(completed, reopened.load());
        }
    }

    private Path checkpoint() { return directory.resolve("nested").resolve("checkpoint.txt"); }

    private static ZedToken token(String value) { return ZedToken.newBuilder().setToken(value).build(); }
}
