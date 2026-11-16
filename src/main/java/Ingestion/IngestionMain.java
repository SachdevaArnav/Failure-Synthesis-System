
package Ingestion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import Storage.BlockAllocator;
import Storage.BlockMetadataStore;
import Storage.BlockStore;
import Storage.SegmentManager;
import Storage.SegmentMetadataStore;
import Storage.StorageConfig;
import Storage.StorageWorker;
import Storage.TelemetryBuffer;

public final class IngestionMain {

    public static void main(String[] args) throws Exception {

        Path propertiesPath = Path.of("storage.properties");

        String brokerHost = "localhost";
        int brokerPort = 1883;

        int subscriberCount = 4;
        String topicFilter = "$share/failure-synthesis/fleet/telemetry";

        // Existing storage initialization
        StorageConfig config = StorageConfig.load(propertiesPath);
        Files.createDirectories(config.root);

        BlockStore blockStore = new BlockStore(config.root.resolve("blocks.dat"));

        BlockMetadataStore blockMetadataStore = new BlockMetadataStore(
                config.root.resolve("block-metadata.dat"),
                config.blockSize);

        SegmentMetadataStore segmentMetadataStore = new SegmentMetadataStore(config.root);

        BlockAllocator allocator = new BlockAllocator(config.blockSize, blockMetadataStore);

        SegmentManager segmentManager = new SegmentManager(
                config,
                blockStore,
                blockMetadataStore,
                allocator,
                segmentMetadataStore);

        TelemetryBuffer buffer = new TelemetryBuffer(config.bufferCapacityBytes);

        StorageWorker storageWorker = new StorageWorker(config, buffer, segmentManager);

        // Start storage consumer
        Thread storageThread = Thread.ofVirtual().start(() -> {
            try {
                storageWorker.processUntilClosed();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Shared ingestion pipeline
        TelemetryIngestor ingestor = new TelemetryIngestor(
                buffer,
                config.envelope,
                config.maxMessageSize);

        // Start four independent MQTT subscribers
        List<MqttTransportAdapter> transports = new ArrayList<>();

        for (int i = 1; i <= subscriberCount; i++) {
            MqttTransportAdapter transport = new MqttTransportAdapter(
                    brokerHost,
                    brokerPort,
                    "failure-synthesis-" + i,
                    topicFilter,
                    ingestor);

            transports.add(transport);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            System.out.println("\nShutting down...");

            for (MqttTransportAdapter transport : transports) {
                try {
                    transport.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            System.out.println("\n========== MQTT SUBSCRIBER STATS ==========");

            for (MqttTransportAdapter transport : transports) {
                transport.printStats();
            }

            System.out.println("===========================================\n");

            // Stop accepting messages and drain remaining buffer data.
            buffer.close();

            try {
                storageThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            System.out.println("Storage worker finished.");
        }));

        // Connect all subscribers
        for (MqttTransportAdapter transport : transports) {
            transport.start();
        }

        System.out.println("Started " + subscriberCount + " MQTT subscribers.");

        // Keep application alive until Ctrl+C.
        new CountDownLatch(1).await();
    }
}
