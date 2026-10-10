package Storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class SegmentMetadataStore implements AutoCloseable {
    public record Segment(long startTime, long endTime) {
    }

    private static final int SEGMENT_RECORD_SIZE = 16;
    private final Path root;
    private final Path segmentsDirectory;
    private final Path cataloguePath;
    private final Map<Long, Segment> catalogue = new TreeMap<>();
    private final FileChannel catalogueChannel;

    public SegmentMetadataStore(Path root) throws IOException {
        this.root = root;
        this.segmentsDirectory = root.resolve("segments");
        this.cataloguePath = root.resolve("segments.index");

        Files.createDirectories(segmentsDirectory);
        catalogueChannel = FileChannel.open(cataloguePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC);
        loadCatalogue();
    }

    private void loadCatalogue() throws IOException {
        long size = catalogueChannel.size();
        long complete = size / SEGMENT_RECORD_SIZE;
        if (size % SEGMENT_RECORD_SIZE != 0) {
            catalogueChannel.truncate(complete * SEGMENT_RECORD_SIZE);
        }

        for (long i = 0; i < complete; i++) {
            ByteBuffer b = ByteBuffer.allocate(SEGMENT_RECORD_SIZE)
                    .order(ByteOrder.BIG_ENDIAN);
            readFully(catalogueChannel, b, i * SEGMENT_RECORD_SIZE);
            b.flip();

            long start = b.getLong();
            long end = b.getLong();
            if (end <= start || catalogue.containsKey(start)) {
                throw new IOException("Invalid segment catalogue entry");
            }
            catalogue.put(start, new Segment(start, end));
        }
    }

    public synchronized void ensureSegment(long start, long end)
            throws IOException {
        Segment existing = catalogue.get(start);
        if (existing != null) {
            if (existing.endTime() != end) {
                throw new IOException("Conflicting segment boundaries");
            }
            return;
        }

        ByteBuffer b = ByteBuffer.allocate(SEGMENT_RECORD_SIZE)
                .order(ByteOrder.BIG_ENDIAN);
        b.putLong(start).putLong(end).flip();

        writeFully(catalogueChannel, b, catalogueChannel.size());
        catalogue.put(start, new Segment(start, end));
    }

    public synchronized List<Segment> getSegments() {
        return Collections.unmodifiableList(new ArrayList<>(catalogue.values()));
    }

    public synchronized List<Long> readBlockIds(long segmentStart, long agentId)
            throws IOException {
        Path path = mappingPath(segmentStart, agentId);
        if (!Files.exists(path))
            return new ArrayList<>();

        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long size = channel.size();
            long complete = size / Long.BYTES;
            if (size % Long.BYTES != 0) {
                channel.truncate(complete * Long.BYTES);
            }

            List<Long> ids = new ArrayList<>();
            for (long i = 0; i < complete; i++) {
                ByteBuffer b = ByteBuffer.allocate(Long.BYTES)
                        .order(ByteOrder.BIG_ENDIAN);
                readFully(channel, b, i * Long.BYTES);
                b.flip();
                long id = b.getLong();
                if (id < 0)
                    throw new IOException("Invalid block ID in mapping");
                ids.add(id);
            }
            return ids;
        }
    }

    public synchronized void addBlockMapping(
            long segmentStart, long agentId, long blockId) throws IOException {
        if (blockId < 0)
            throw new IllegalArgumentException("Negative block ID");

        Path path = mappingPath(segmentStart, agentId);
        Files.createDirectories(path.getParent());

        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.DSYNC)) {
            ByteBuffer b = ByteBuffer.allocate(Long.BYTES)
                    .order(ByteOrder.BIG_ENDIAN);
            b.putLong(blockId).flip();
            writeFully(channel, b, channel.size());
        }
    }

    private Path mappingPath(long segmentStart, long agentId) {
        return segmentsDirectory
                .resolve("segment-" + segmentStart)
                .resolve("agent-" + Long.toUnsignedString(agentId) + ".map");
    }

    private static void writeFully(FileChannel channel, ByteBuffer b, long pos)
            throws IOException {
        while (b.hasRemaining()) {
            int n = channel.write(b, pos);
            if (n < 0)
                throw new IOException("Unexpected write failure");
            if (n == 0) {
                Thread.onSpinWait();
                continue;
            }
            pos += n;
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer b, long pos)
            throws IOException {
        while (b.hasRemaining()) {
            int n = channel.read(b, pos);
            if (n < 0)
                throw new EOFException("Incomplete metadata");
            if (n == 0) {
                Thread.onSpinWait();
                continue;
            }
            pos += n;
        }
    }

    @Override
    public void close() throws IOException {
        catalogueChannel.close();
    }
}
