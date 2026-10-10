package Storage;

import java.io.IOException;

public final class BlockAllocator {
    private final int blockSize;
    private final BlockMetadataStore metadataStore;
    private long nextId;

    public BlockAllocator(int blockSize, BlockMetadataStore metadataStore)
            throws IOException {
        if (blockSize <= 0)
            throw new IllegalArgumentException("Invalid block size");
        this.blockSize = blockSize;
        this.metadataStore = metadataStore;
        this.nextId = metadataStore.nextBlockId();
    }

    public synchronized BlockMetadata allocate() throws IOException {
        long id = nextId;
        long offset = Math.multiplyExact(id, (long) blockSize);

        BlockMetadata metadata = BlockMetadata.empty(id, offset);
        metadataStore.write(metadata);
        nextId++;
        return metadata;
    }
}
