package Storage;

public record BlockMetadata(
        long blockId,
        long storageOffset,
        int usedLength,
        long startTime,
        long endTime) {

    public static BlockMetadata empty(long blockId, long storageOffset) {
        return new BlockMetadata(
                blockId, storageOffset, 0, Long.MAX_VALUE, Long.MIN_VALUE);
    }

    public boolean canFit(int recordLength, int blockSize) {
        return recordLength >= 0
                && recordLength <= blockSize - usedLength;
    }

    public BlockMetadata withRecord(int recordLength, long timestamp) {
        if (recordLength < 0 || usedLength > Integer.MAX_VALUE - recordLength) {
            throw new IllegalArgumentException("Invalid record length");
        }

        return new BlockMetadata(
                blockId,
                storageOffset,
                usedLength + recordLength,
                Math.min(startTime, timestamp),
                Math.max(endTime, timestamp));
    }
}
