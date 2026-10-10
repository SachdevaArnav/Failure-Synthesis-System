package Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SegmentManager {
    private record AgentSegment(long segmentStart, long agentId) {
    }

    private final StorageConfig config;
    private final BlockStore blockStore;
    private final BlockMetadataStore metadataStore;
    private final BlockAllocator allocator;
    private final SegmentMetadataStore segmentStore;

    private final Map<AgentSegment, BlockMetadata> currentBlocks = new HashMap<>();

    public SegmentManager(
            StorageConfig config,
            BlockStore blockStore,
            BlockMetadataStore metadataStore,
            BlockAllocator allocator,
            SegmentMetadataStore segmentStore) {
        this.config = config;
        this.blockStore = blockStore;
        this.metadataStore = metadataStore;
        this.allocator = allocator;
        this.segmentStore = segmentStore;
    }

    public synchronized void append(
            long agentId, long timestamp, ByteBuffer record) throws IOException {
        int recordLength = record.remaining();
        if (recordLength <= 0 || recordLength > config.blockSize) {
            throw new IOException("Record cannot fit in a storage block");
        }

        long segmentStart = Math.multiplyExact(
                Math.floorDiv(timestamp, config.segmentDurationMillis),
                config.segmentDurationMillis);
        long segmentEnd = Math.addExact(segmentStart, config.segmentDurationMillis);

        segmentStore.ensureSegment(segmentStart, segmentEnd);

        AgentSegment key = new AgentSegment(segmentStart, agentId);
        BlockMetadata block = currentBlocks.get(key);

        if (block == null) {
            List<Long> ids = segmentStore.readBlockIds(segmentStart, agentId);
            if (!ids.isEmpty()) {
                block = metadataStore.read(ids.get(ids.size() - 1));
            }
        }

        boolean newBlock = false;
        if (block == null || !block.canFit(recordLength, config.blockSize)) {
            block = allocator.allocate();
            newBlock = true;
        }

        long writeOffset = Math.addExact(block.storageOffset(), block.usedLength());

        // Write the original incoming bytes. Do not reconstruct the envelope.
        blockStore.write(record.duplicate(), writeOffset);

        BlockMetadata updated = block.withRecord(recordLength, timestamp);
        metadataStore.write(updated);

        if (newBlock) {
            segmentStore.addBlockMapping(segmentStart, agentId, block.blockId());
        }

        currentBlocks.put(key, updated);
    }
}
