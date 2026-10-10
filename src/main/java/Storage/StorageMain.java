package Storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class StorageMain {
    public static void main(String[] args) throws Exception {
        Path propertiesPath = Path.of(
                args.length > 0 ? args[0] : "storage.properties");

        StorageConfig config = StorageConfig.load(propertiesPath);
        Files.createDirectories(config.root);

        BlockStore blockStore = new BlockStore(config.root.resolve("blocks.dat"));
        BlockMetadataStore metadataStore = new BlockMetadataStore(
                config.root.resolve("block-metadata.dat"));
        SegmentMetadataStore segmentStore = new SegmentMetadataStore(config.root);

        BlockAllocator allocator = new BlockAllocator(config.blockSize, metadataStore);

        SegmentManager segmentManager = new SegmentManager(
                config, blockStore, metadataStore,
                allocator, segmentStore);

        TelemetryBuffer buffer = new TelemetryBuffer(config.bufferCapacityBytes);

        StorageWorker worker = new StorageWorker(config, buffer, segmentManager);

        Thread workerThread = new Thread(() -> {
            try {
                worker.processUntilClosed();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, "telemetry-storage-worker");

        workerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            buffer.close();
            try {
                workerThread.join();
                blockStore.close();
                metadataStore.close();
                segmentStore.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }

    private StorageMain() {
    }
}
