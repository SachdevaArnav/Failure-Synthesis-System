package Storage;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

public final class StorageMain {
    public static void main(String[] args) throws Exception {
        Path propertiesPath = Path.of(
                args.length > 0 ? args[0] : "storage.properties");

        StorageConfig config = StorageConfig.load(propertiesPath);
        Files.createDirectories(config.root);

        BlockStore blockStore = new BlockStore(config.root.resolve("blocks.dat"));
        BlockMetadataStore metadataStore = new BlockMetadataStore(
                config.root.resolve("block-metadata.dat"), config.blockSize);
        SegmentMetadataStore segmentStore = new SegmentMetadataStore(config.root);

        BlockAllocator allocator = new BlockAllocator(config.blockSize, metadataStore);

        SegmentManager segmentManager = new SegmentManager(
                config, blockStore, metadataStore,
                allocator, segmentStore);

        TelemetryBuffer buffer = new TelemetryBuffer(config.bufferCapacityBytes);

        StorageWorker worker = new StorageWorker(config, buffer, segmentManager);

        AtomicReference<Throwable> workerFailure = new AtomicReference<>();

        Thread workerThread = new Thread(() -> {
            try {
                worker.processUntilClosed();
            } catch (Throwable e) {
                workerFailure.set(e);
            }
        }, "telemetry-storage-worker");

        try {
            workerThread.start();

            // One test message for Agent 7
            // Multi-agent test: 6 messages across 3 agents
            long timestamp = System.currentTimeMillis();

            long[] agentIds = { 7, 12, 25, 7, 12, 25 };

            for (int i = 0; i < agentIds.length; i++) {
                ByteBuffer message = ByteBuffer.allocate(24)
                        .order(config.envelope.byteOrder());

                message.putInt((int) agentIds[i]);
                message.putLong(timestamp + i);
                message.putInt(4);
                message.putInt(1);
                message.put(new byte[] {
                        (byte) (0x10 + i),
                        (byte) (0x20 + i),
                        (byte) (0x30 + i),
                        (byte) (0x40 + i)
                });

                message.flip();
                buffer.offer(message);
            }

            buffer.close();
            workerThread.join();
            if (workerFailure.get() != null) {
                throw new RuntimeException("Storage worker failed", workerFailure.get());
            }

            System.out.println("Smoke test completed.");

        } finally {
            buffer.close();
            workerThread.join();
            blockStore.close();
            metadataStore.close();
        }
    }

    private StorageMain() {
    }
}
