package Storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

public final class BlockMetadataStore implements AutoCloseable {
    private static final int RECORD_SIZE = 44;

    private final FileChannel channel;
    private final int blockSize;

    public BlockMetadataStore(Path path, int blockSize) throws IOException {
        channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC);
        this.blockSize = blockSize;
    }

    public synchronized void write(BlockMetadata metadata) throws IOException {
        validate(metadata);

        ByteBuffer b = ByteBuffer.allocate(RECORD_SIZE).order(ByteOrder.BIG_ENDIAN);
        b.putLong(metadata.blockId());
        b.putLong(metadata.storageOffset());
        b.putInt(metadata.usedLength());
        b.putLong(metadata.startTime());
        b.putLong(metadata.endTime());
        b.flip();
        CRC32 checksum = new CRC32();
        checksum.update(b);
        b.position(b.limit());
        b.limit(b.capacity());
        b.putLong(checksum.getValue());
        b.flip();
        // here we r using multiplyExact to safeguard the stored data from silent
        // corruption due to numeric overflow
        writeFully(b, Math.multiplyExact(metadata.blockId(), (long) RECORD_SIZE));
    }

    public synchronized BlockMetadata read(long blockId) throws IOException {
        if (blockId < 0) {
            throw new IllegalArgumentException("Negative block ID");
        }

        long offset = Math.multiplyExact(blockId, (long) RECORD_SIZE);
        if (channel.size() < offset + RECORD_SIZE) {
            throw new EOFException("Missing metadata for block " + blockId);
        }

        ByteBuffer b = ByteBuffer.allocate(RECORD_SIZE).order(ByteOrder.BIG_ENDIAN);
        readFully(b, offset);
        b.rewind();
        BlockMetadata metadata = new BlockMetadata(
                b.getLong(), b.getLong(), b.getInt(), b.getLong(),
                b.getLong());
        long checksum = b.getLong();
        b.rewind();
        b.limit(RECORD_SIZE - 8);
        CRC32 check = new CRC32();
        check.update(b);
        if (checksum != check.getValue()) {
            throw new IOException("Checksum failed");
        }
        if (metadata.blockId() != blockId) {
            throw new IOException("Block metadata ID mismatch at " + blockId);
        }
        validate(metadata);
        return metadata;
    }

    public synchronized long nextBlockId() throws IOException {
        long size = channel.size();
        long completeRecords = size / RECORD_SIZE;
        long remainder = size % RECORD_SIZE;

        if (remainder != 0) {
            channel.truncate(completeRecords * RECORD_SIZE);
        }

        return completeRecords;
    }

    private void validate(BlockMetadata m) throws IOException {
        if (m.blockId() < 0 || m.storageOffset() < 0 || m.usedLength() < 0) {
            throw new IOException("Invalid block metadata");
        }
        if (m.usedLength() == 0) {
            if (m.startTime() != Long.MAX_VALUE || m.endTime() != Long.MIN_VALUE) {
                throw new IOException("Invalid empty-block timestamps");
            }
        } else if (m.startTime() > m.endTime()) {
            throw new IOException("Invalid block time range");
        }
        if (m.usedLength() > blockSize) {
            throw new IOException("Used Length greater than Maximum Block Size not possible");
        }
        if (m.storageOffset() != m.blockId() * blockSize) {
            throw new IOException("Invalid Storage Offset");
        }
    }

    private void writeFully(ByteBuffer b, long position) throws IOException {
        while (b.hasRemaining()) {
            int n = channel.write(b, position);
            if (n < 0)
                throw new IOException("Unexpected write failure");
            if (n == 0) {
                Thread.onSpinWait();
                continue;
            }
            position += n;
        }
    }

    private void readFully(ByteBuffer b, long position) throws IOException {
        while (b.hasRemaining()) {
            int n = channel.read(b, position);
            if (n < 0)
                throw new EOFException("Incomplete block metadata");
            if (n == 0) {
                Thread.onSpinWait();
                continue;
            }
            position += n;
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
