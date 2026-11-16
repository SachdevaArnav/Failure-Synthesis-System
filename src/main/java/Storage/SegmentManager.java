package Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

public final class SegmentManager {
    private record AgentSegment(long segmentStart, long agentId) {
    }

    // LinkedHashMap is not synchronized, and even get() calls modify its internal
    // linked list structure when accessOrder is true.
    private class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private final int maxCapacity;

        public LRUCache(int maxCapacity) {
            // Set initial capacity, standard load factor (0.75f), and accessOrder = true
            super((int) Math.ceil(maxCapacity / 0.75) + 1, 0.75f, true);
            this.maxCapacity = maxCapacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            // Automatically removes the least-recently used entry when size exceeds
            // maxCapacity
            return size() > maxCapacity;
        }
    }

    private final StorageConfig config;
    private final BlockStore blockStore;
    private final BlockMetadataStore metadataStore;
    private final BlockAllocator allocator;
    private final SegmentMetadataStore segmentStore;
    // implmenting the currentBlocks map as LRU Cache to avoid memory leak
    // (unbounded) and still get the advantage of caching
    private final Map<AgentSegment, BlockMetadata> currentBlocks = new LRUCache<>(10_000);
    private final ByteBuffer tempBuffer;

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
        tempBuffer = ByteBuffer.allocate(config.blockSize);
    }

    public synchronized void append(
            long agentId, long timestamp, ByteBuffer record) throws IOException {
        int recordLength = record.remaining();
        if (recordLength <= 0 || recordLength + Long.BYTES > config.blockSize) {
            throw new IOException("Record cannot fit in a storage block");
        }

        long segmentStart = Math.multiplyExact(
                Math.floorDiv(timestamp, config.segmentDurationMillis),
                config.segmentDurationMillis);

        AgentSegment key = new AgentSegment(segmentStart, agentId);
        BlockMetadata block = currentBlocks.get(key);

        if (block == null) {
            List<Long> ids = segmentStore.readBlockIds(segmentStart, agentId);
            if (!ids.isEmpty()) {
                block = metadataStore.read(ids.get(ids.size() - 1));
            }
        }

        boolean newBlock = false;
        if (block == null || !block.canFit(recordLength + Long.BYTES, config.blockSize)) {
            block = allocator.allocate();
            newBlock = true;
        }

        long writeOffset = Math.addExact(block.storageOffset(), block.usedLength());

        // Before writing prepend checksum value in front of the original message
        CRC32 checksum = new CRC32();
        checksum.update(record);
        record.rewind();
        tempBuffer.clear();
        tempBuffer.putLong(checksum.getValue()).put(record);
        tempBuffer.flip();
        blockStore.write(tempBuffer, writeOffset);

        BlockMetadata updated = block.withRecord(recordLength + Long.BYTES, timestamp);
        metadataStore.write(updated);

        if (newBlock) {
            segmentStore.addBlockMapping(segmentStart, agentId, block.blockId());
        }

        currentBlocks.put(key, updated);
    }
}
