package Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public final class SmokeTest {
    private static final long AGENT_ID = 7L;
    private static final int RECORD_SIZE = 24;

    public static void main(String[] args) throws Exception {
        Path propertiesPath = Path.of(
                args.length > 0 ? args[0] : "storage.properties");

        StorageConfig config = StorageConfig.load(propertiesPath);
        Files.createDirectories(config.root);

        try (BlockStore blockStore =
                     new BlockStore(config.root.resolve("blocks.dat"));
             BlockMetadataStore metadataStore =
                     new BlockMetadataStore(config.root.resolve("block-metadata.dat"));
             SegmentMetadataStore segmentStore =
                     new SegmentMetadataStore(config.root)) {

            BlockAllocator allocator =
                    new BlockAllocator(config.blockSize, metadataStore);
            SegmentManager manager =
                    new SegmentManager(config, blockStore, metadataStore,
                            allocator, segmentStore);
            TelemetryBuffer buffer =
                    new TelemetryBuffer(config.bufferCapacityBytes);
            StorageWorker worker =
                    new StorageWorker(config, buffer, manager);

            byte[] r1 = record(AGENT_ID, 1_000L, 10, 111);
            byte[] r2 = record(AGENT_ID, 2_000L, 11, 222);
            byte[] r3 = record(AGENT_ID, 120_001L, 12, 333);

            AtomicReference<Throwable> workerFailure = new AtomicReference<>();
            Thread thread = new Thread(() -> {
                try {
                    worker.processUntilClosed();
                } catch (Throwable t) {
                    workerFailure.set(t);
                }
            }, "smoke-test-storage-worker");

            thread.start();
            buffer.offer(ByteBuffer.wrap(r1));
            buffer.offer(ByteBuffer.wrap(r2));
            buffer.offer(ByteBuffer.wrap(r3));
            buffer.close();
            thread.join();

            if (workerFailure.get() != null) {
                throw new AssertionError("StorageWorker failed",
                        workerFailure.get());
            }

            // The first two 24-byte records share block 0.
            // The third record is in a later segment and uses block 1.
            BlockMetadata block0 = metadataStore.read(0);
            BlockMetadata block1 = metadataStore.read(1);

            check(block0.usedLength() == 48,
                    "Block 0 usedLength should be 48, got " + block0.usedLength());
            check(block1.usedLength() == 24,
                    "Block 1 usedLength should be 24, got " + block1.usedLength());
            check(block0.startTime() == 1_000L && block0.endTime() == 2_000L,
                    "Block 0 timestamp range is incorrect");
            check(block1.startTime() == 120_001L && block1.endTime() == 120_001L,
                    "Block 1 timestamp range is incorrect");

            assertStoredBytes(blockStore, 0L, r1);
            assertStoredBytes(blockStore, RECORD_SIZE, r2);
            assertStoredBytes(blockStore, 64L, r3);

            List<Long> firstSegmentBlocks = segmentStore.readBlockIds(0L, AGENT_ID);
            List<Long> secondSegmentBlocks = segmentStore.readBlockIds(120_000L, AGENT_ID);
            check(firstSegmentBlocks.equals(List.of(0L)),
                    "Unexpected first-segment mapping: " + firstSegmentBlocks);
            check(secondSegmentBlocks.equals(List.of(1L)),
                    "Unexpected second-segment mapping: " + secondSegmentBlocks);
            check(segmentStore.getSegments().size() == 2,
                    "Expected two segments");

            System.out.println("PASS: all three original records were stored byte-for-byte.");
            System.out.println("PASS: block 0 usedLength = " + block0.usedLength());
            System.out.println("PASS: block 1 usedLength = " + block1.usedLength());
            System.out.println("PASS: two segments and agent-to-block mappings verified.");
            System.out.println("Storage directory: " + config.root.toAbsolutePath());
        }
    }

    private static byte[] record(long agentId, long timestamp,
                                 int eventType, int payloadMarker) {
        ByteBuffer b = ByteBuffer.allocate(RECORD_SIZE).order(ByteOrder.BIG_ENDIAN);
        b.putInt((int) agentId); // UINT32
        b.putLong(timestamp);    // INT64
        b.putInt(4);             // payload length
        b.putInt(eventType);     // UINT32 event type
        b.putInt(payloadMarker); // 4-byte payload
        return b.array();
    }

    private static void assertStoredBytes(BlockStore store, long offset,
                                          byte[] expected) throws IOException {
        ByteBuffer actual = ByteBuffer.allocate(expected.length);
        store.readFully(actual, offset);
        check(Arrays.equals(actual.array(), expected),
                "Stored bytes differ from original at offset " + offset);
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private SmokeTest() {}
}
