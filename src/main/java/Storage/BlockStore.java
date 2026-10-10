package Storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Low-level positional byte storage backed by blocks.dat.
 *
 * This class knows nothing about telemetry, segments, or logical blocks.
 */
public final class BlockStore implements AutoCloseable {

    private final FileChannel channel;

    public BlockStore(Path path) throws IOException {
        Objects.requireNonNull(path, "path");
        this.channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC);
    }

    /**
     * Writes all remaining bytes from src starting at storageOffset.
     * Advances src.position() as bytes are written.
     */
    public void write(ByteBuffer src, long storageOffset)
            throws IOException {

        Objects.requireNonNull(src, "src");
        validateRange(storageOffset, src.remaining());

        long position = storageOffset;

        while (src.hasRemaining()) {
            int written = channel.write(src, position);

            if (written == 0) {
                Thread.onSpinWait();
                continue;
            }

            position += written;
        }
    }

    /**
     * Reads exactly dst.remaining() bytes starting at storageOffset.
     * Advances dst.position() as bytes are read.
     *
     * Throws EOFException if the requested range is incomplete.
     */
    public void readFully(ByteBuffer dst, long storageOffset)
            throws IOException {

        Objects.requireNonNull(dst, "dst");
        validateRange(storageOffset, dst.remaining());

        long position = storageOffset;

        while (dst.hasRemaining()) {
            int bytesRead = channel.read(dst, position);

            if (bytesRead < 0) {
                throw new EOFException(
                        "Unexpected end of file at offset " + position);
            }

            if (bytesRead == 0) {
                Thread.onSpinWait();
                continue;
            }

            position += bytesRead;
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private static void validateRange(long offset, int length) {
        if (offset < 0) {
            throw new IllegalArgumentException(
                    "Storage offset must be non-negative");
        }

        if (length < 0 || offset > Long.MAX_VALUE - length) {
            throw new IllegalArgumentException(
                    "Invalid storage range");
        }
    }
}
