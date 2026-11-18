
package Retrieval;

import java.nio.file.Path;
import java.util.List;

import Storage.BlockMetadataStore;
import Storage.BlockStore;
import Storage.SegmentMetadataStore;
import Storage.StorageConfig;

public final class RetrievalTest {

    public static void main(String[] args) throws Exception {

        Path propertiesPath = Path.of(
                args.length > 0 ? args[0] : "storage.properties");

        StorageConfig config = StorageConfig.load(propertiesPath);

        BlockStore blockStore = new BlockStore(
                config.root.resolve("blocks.dat"));

        BlockMetadataStore metadataStore = new BlockMetadataStore(
                config.root.resolve("block-metadata.dat"),
                config.blockSize);

        SegmentMetadataStore segmentStore = new SegmentMetadataStore(config.root);

        try {
            RetrievalService service = new RetrievalService(
                    config,
                    blockStore,
                    metadataStore,
                    segmentStore);

            // EDIT THESE VALUES FOR YOUR TEST DATA.
            boolean allSources = true;
            long agentId = 1L;
            long startTime = 0L;
            long endTime = Long.MAX_VALUE;
            Long eventType = null;

            List<RetrievalRecord> records = service.retrieve(
                    allSources,
                    agentId,
                    startTime,
                    endTime,
                    eventType);

            System.out.println("Retrieved records: " + records.size());

            for (RetrievalRecord record : records) {
                System.out.println("-------------------------");
                System.out.println("Agent ID:  " + record.agentId());
                System.out.println("Timestamp: " + record.timestamp());
                System.out.println("Event Type:" + record.eventType());
                System.out.println("Payload:   " + record.payload());
            }

        } finally {
            metadataStore.close();
            blockStore.close();
        }
    }
}
