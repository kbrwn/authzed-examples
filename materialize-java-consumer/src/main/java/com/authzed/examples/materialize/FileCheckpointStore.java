package com.authzed.examples.materialize;

import com.authzed.api.v1.ZedToken;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

/** Requires a persistent filesystem supporting atomic rename and directory fsync. */
public final class FileCheckpointStore implements AsyncMaterializeConsumer.CheckpointStore, AutoCloseable {
    private final Path file;
    private final String identity;
    private final FileChannel lockChannel;
    private final FileLock lock;

    public FileCheckpointStore(Path file, String identity) throws IOException {
        this.file = file.toAbsolutePath();
        this.identity = Base64.getEncoder().encodeToString(identity.getBytes(StandardCharsets.UTF_8));
        Files.createDirectories(this.file.getParent());
        lockChannel = FileChannel.open(this.file.resolveSibling(this.file.getFileName() + ".lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock candidate;
        try { candidate = lockChannel.tryLock(); }
        catch (RuntimeException | IOException failure) { lockChannel.close(); throw failure; }
        if (candidate == null) {
            lockChannel.close();
            throw new IOException("Another consumer owns this checkpoint");
        }
        lock = candidate;
        try { load(); }
        catch (IOException | RuntimeException failure) {
            try { close(); } catch (IOException closing) { failure.addSuppressed(closing); }
            throw failure;
        }
    }

    public ZedToken load() throws IOException {
        ensureOpen();
        if (!Files.exists(file)) return null;
        var lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        if (lines.size() != 2 || !lines.get(0).equals(identity) || lines.get(1).isBlank()) {
            throw new IOException("Checkpoint identity/content does not match source and destination");
        }
        return ZedToken.newBuilder().setToken(lines.get(1)).build();
    }

    @Override public void save(ZedToken revision) throws IOException {
        ensureOpen();
        String token = revision.getToken();
        if (token.isBlank() || token.indexOf('\n') >= 0 || token.indexOf('\r') >= 0) {
            throw new IOException("Invalid checkpoint token");
        }
        Path temporary = Files.createTempFile(file.getParent(), ".checkpoint-", ".tmp");
        try {
            try (var output = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                var contents = ByteBuffer.wrap((identity + "\n" + token + "\n").getBytes(StandardCharsets.UTF_8));
                while (contents.hasRemaining()) output.write(contents);
                output.force(true);
            }
            Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (var directory = FileChannel.open(file.getParent(), StandardOpenOption.READ)) {
                directory.force(true);
            }
        } finally { Files.deleteIfExists(temporary); }
    }

    @Override public void close() throws IOException {
        try { if (lock.isValid()) lock.release(); } finally { lockChannel.close(); }
    }

    private void ensureOpen() throws IOException {
        if (!lock.isValid()) throw new IOException("Checkpoint store is closed");
    }
}
