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
import java.util.List;

public final class SegmentMetadataStore {
    public record Segment(long startTime, long endTime) {
    }

    private static final int SEGMENT_RECORD_SIZE = 16;
    private final Path root;
    private final Path segmentsDirectory;

    public SegmentMetadataStore(Path root) throws IOException {
        this.root = root;
        this.segmentsDirectory = root.resolve("segments");

        Files.createDirectories(segmentsDirectory);
    }

    public List<Long> readBlockIds(long segmentStart, long agentId)
            throws IOException {
        Path path = mappingPath(segmentStart, agentId);
        if (!Files.exists(path))
            return new ArrayList<>();

        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.READ)) {
            long size = channel.size();
            long complete = size / Long.BYTES;
            // Since all ids should be in long so trimming any extra tail bytes
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
                if (id >= 0)
                    ids.add(id);
                // negative ids r invalid --> further checks of validity of id will be handled
                // by BlockMetaDataStore
                // we just skip invalid values (if some corrupt data passed here as seemingly
                // valid the read BlockMetaDataStore Tries to catch them but its not guranteed)
            }
            return ids;
        }
    }

    public synchronized void addBlockMapping(
            long segmentStart, long agentId, long blockId) throws IOException {
        // When you actually introduce multiple writers, replace this coarse lock
        // (synchronized) with per-file or striped locking
        // in that we need to think about remove older locks in some way to avoid memory
        // leakage but also need to take care regarding the segment boundary being
        // defined by timestamps of the agents which is not global or sync time which
        // can complicate things pretty fast
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

}
