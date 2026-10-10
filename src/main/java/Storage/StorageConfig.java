package Storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public final class StorageConfig {
    public final Path root;
    public final int blockSize;
    public final int bufferCapacityBytes;
    public final int maxMessageSize;
    public final long segmentDurationMillis;
    public final EnvelopeConfig envelope;

    private StorageConfig(
            Path root, int blockSize, int bufferCapacityBytes,
            int maxMessageSize, long segmentDurationMillis,
            EnvelopeConfig envelope) {
        this.root = root;
        this.blockSize = blockSize;
        this.bufferCapacityBytes = bufferCapacityBytes;
        this.maxMessageSize = maxMessageSize;
        this.segmentDurationMillis = segmentDurationMillis;
        this.envelope = envelope;
    }

    public static StorageConfig load(Path propertiesPath) throws IOException {
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(propertiesPath)) {
            p.load(in);
        }

        Path base = propertiesPath.toAbsolutePath().getParent();
        Path root = resolve(base, required(p, "root"));
        Path envelopePath = resolve(base, required(p, "envelopeConfig"));

        int blockSize = integer(p, "blockSize");
        int bufferCapacity = integer(p, "bufferCapacityBytes");
        int maxMessage = integer(p, "maxMessageSize");
        long segmentDuration = longValue(p, "segmentDurationMillis");

        ByteOrder order = switch (p.getProperty("byteOrder", "BIG_ENDIAN").trim().toUpperCase()) {
            case "BIG_ENDIAN" -> ByteOrder.BIG_ENDIAN;
            case "LITTLE_ENDIAN" -> ByteOrder.LITTLE_ENDIAN;
            default -> throw new IOException("Invalid byteOrder");
        };
        if (maxMessage > blockSize) {
            throw new IOException(
                    "maxMessageSize must not exceed blockSize");
        }

        EnvelopeConfig envelope = EnvelopeConfig.load(envelopePath, order);
        envelope.validateAgainstMaxMessageSize(maxMessage);
        if (blockSize <= 0 || maxMessage <= 0 || segmentDuration <= 0) {
            throw new IOException("Block size, message size and segment duration must be positive");
        }
        if (bufferCapacity < maxMessage) {
            throw new IOException("bufferCapacityBytes must be >= maxMessageSize");
        }

        return new StorageConfig(root, blockSize, bufferCapacity,
                maxMessage, segmentDuration, envelope);
    }

    private static Path resolve(Path base, String value) {
        Path path = Path.of(value);
        return path.isAbsolute() ? path : base.resolve(path).normalize();
    }

    private static String required(Properties p, String key) throws IOException {
        String value = p.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IOException("Missing property: " + key);
        }
        return value.trim();
    }

    private static int integer(Properties p, String key) throws IOException {
        try {
            return Integer.parseInt(required(p, key));
        } catch (NumberFormatException e) {
            throw new IOException("Invalid integer property: " + key, e);
        }
    }

    private static long longValue(Properties p, String key) throws IOException {
        try {
            return Long.parseLong(required(p, key));
        } catch (NumberFormatException e) {
            throw new IOException("Invalid long property: " + key, e);
        }
    }
}
