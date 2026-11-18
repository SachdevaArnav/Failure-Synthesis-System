package Retrieval;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import Storage.BlockMetadata;
import Storage.BlockMetadataStore;
import Storage.BlockStore;
import Storage.EnvelopeConfig;
import Storage.SegmentMetadataStore;
import Storage.StorageConfig;

public final class RetrievalService {

    private record Mapping(long agentId, Path path) {
    }

    private final StorageConfig config;
    private final BlockStore blockStore;
    private final BlockMetadataStore metadataStore;
    private final Path segmentsDirectory;
    private final EnvelopeConfig envelope;

    public RetrievalService(
            StorageConfig config,
            BlockStore blockStore,
            BlockMetadataStore metadataStore,
            SegmentMetadataStore segmentStore) {

        this.config = config;
        this.blockStore = blockStore;
        this.metadataStore = metadataStore;
        this.segmentsDirectory = config.root.resolve("segments");
        this.envelope = config.envelope;
    }

    public List<RetrievalRecord> retrieve(
            boolean allSources,
            long agentId,
            long startTime,
            long endTime,
            Long eventType) throws IOException {

        if (startTime > endTime) {
            throw new IllegalArgumentException("Invalid time range");
        }

        List<RetrievalRecord> results = new ArrayList<>();

        if (!Files.exists(segmentsDirectory)) {
            return results;
        }

        try (Stream<Path> segments = Files.list(segmentsDirectory)) {
            for (Path segment : segments.toList()) {
                if (!Files.isDirectory(segment)) {
                    continue;
                }

                for (Mapping mapping : findMappings(segment, allSources, agentId)) {
                    List<Long> blockIds = readBlockIds(mapping.path());

                    for (long blockId : blockIds) {
                        BlockMetadata metadata = metadataStore.read(blockId);

                        if (metadata.usedLength() == 0
                                || metadata.endTime() < startTime
                                || metadata.startTime() > endTime) {
                            continue;
                        }

                        readBlock(
                                mapping.agentId(),
                                metadata,
                                startTime,
                                endTime,
                                eventType,
                                results);
                    }
                }
            }
        }

        results.sort(
                Comparator.comparingLong(RetrievalRecord::timestamp)
                        .thenComparing(RetrievalRecord::agentId));

        return results;
    }

    private List<Mapping> findMappings(
            Path segment,
            boolean allSources,
            long requestedAgentId) throws IOException {

        List<Mapping> mappings = new ArrayList<>();

        if (allSources) {
            try (Stream<Path> files = Files.list(segment)) {
                for (Path file : files.toList()) {
                    if (!Files.isRegularFile(file)) {
                        continue;
                    }

                    Long id = parseAgentId(file);
                    if (id != null) {
                        mappings.add(new Mapping(id, file));
                    }
                }
            }
        } else {
            Path mapping = segment.resolve(
                    "agent-" + Long.toUnsignedString(requestedAgentId) + ".map");

            if (Files.isRegularFile(mapping)) {
                mappings.add(new Mapping(requestedAgentId, mapping));
            }
        }

        return mappings;
    }

    private Long parseAgentId(Path file) {
        String name = file.getFileName().toString();

        if (!name.startsWith("agent-") || !name.endsWith(".map")) {
            return null;
        }

        String value = name.substring(6, name.length() - 4);

        try {
            return Long.parseUnsignedLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /*
     * Reads only complete 8-byte block IDs.
     * An incomplete trailing ID is ignored.
     */
    private List<Long> readBlockIds(Path path) throws IOException {
        List<Long> ids = new ArrayList<>();

        try (FileChannel channel = FileChannel.open(
                path, StandardOpenOption.READ)) {

            long complete = channel.size() / Long.BYTES;

            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES)
                    .order(ByteOrder.BIG_ENDIAN);

            for (long i = 0; i < complete; i++) {
                buffer.clear();
                readFully(channel, buffer, i * Long.BYTES);
                buffer.flip();

                long id = buffer.getLong();

                if (id >= 0) {
                    ids.add(id);
                }
            }
        }

        return ids;
    }

    private void readBlock(
            long mappedAgentId,
            BlockMetadata metadata,
            long startTime,
            long endTime,
            Long requestedEventType,
            List<RetrievalRecord> results) throws IOException {

        int usedLength = metadata.usedLength();

        ByteBuffer block = ByteBuffer.allocate(usedLength);
        blockStore.readFully(block, metadata.storageOffset());
        block.flip();

        byte[] bytes = new byte[usedLength];
        block.get(bytes);

        int offset = 0;

        while (offset < usedLength) {
            if (usedLength - offset < Long.BYTES) {
                throw new IOException("Incomplete record checksum in block "
                        + metadata.blockId());
            }

            ByteBuffer checksumBuffer = ByteBuffer.wrap(bytes, offset, Long.BYTES)
                    .order(ByteOrder.BIG_ENDIAN);

            long expectedChecksum = checksumBuffer.getLong();

            int messageOffset = offset + Long.BYTES;

            if (usedLength - messageOffset < envelope.field("Payload").start()) {
                throw new IOException("Incomplete message header in block "
                        + metadata.blockId());
            }

            int messageLength = getMessageLength(bytes, messageOffset);

            if (messageLength <= 0
                    || messageLength > config.maxMessageSize
                    || messageLength > usedLength - messageOffset) {
                throw new IOException("Invalid message length in block "
                        + metadata.blockId());
            }

            byte[] message = new byte[messageLength];
            System.arraycopy(bytes, messageOffset, message, 0, messageLength);

            verifyChecksum(message, expectedChecksum, metadata.blockId());

            RetrievalRecord record = decodeRecord(message, mappedAgentId);

            if (record.timestamp() >= startTime
                    && record.timestamp() <= endTime
                    && (requestedEventType == null
                            || record.eventType() == requestedEventType)) {
                results.add(record);
            }

            offset = messageOffset + messageLength;
        }
    }

    private int getMessageLength(byte[] block, int messageOffset)
            throws IOException {

        EnvelopeConfig.Field payloadLength = envelope.field("Payload Length");
        EnvelopeConfig.Field payload = envelope.field("Payload");

        ByteBuffer b = ByteBuffer.wrap(block)
                .order(envelope.byteOrder());

        long length = Integer.toUnsignedLong(
                b.getInt(messageOffset + payloadLength.start()));

        long totalLength = (long) payload.start() + length;

        if (totalLength > Integer.MAX_VALUE) {
            throw new IOException("Message length overflow");
        }

        return (int) totalLength;
    }

    private RetrievalRecord decodeRecord(
            byte[] message,
            long mappedAgentId) throws IOException {

        ByteBuffer b = ByteBuffer.wrap(message)
                .order(envelope.byteOrder());

        EnvelopeConfig.Field agentField = envelope.field("Agent ID");
        EnvelopeConfig.Field timestampField = envelope.field("Timestamp");
        EnvelopeConfig.Field eventField = envelope.field("Event Type");
        EnvelopeConfig.Field payloadField = envelope.field("Payload");

        long actualAgentId = switch (agentField.type()) {
            case UINT32 -> Integer.toUnsignedLong(b.getInt(agentField.start()));
            case UINT64 -> b.getLong(agentField.start());
            default -> throw new IOException("Unsupported Agent ID type");
        };

        if (actualAgentId != mappedAgentId) {
            throw new IOException("Agent ID does not match segment mapping");
        }

        long timestamp = b.getLong(timestampField.start());

        long eventType = switch (eventField.type()) {
            case UINT32 -> Integer.toUnsignedLong(b.getInt(eventField.start()));
            case INT32 -> b.getInt(eventField.start());
            default -> throw new IOException("Unsupported Event Type");
        };

        byte[] payload = new byte[message.length - payloadField.start()];

        System.arraycopy(
                message,
                payloadField.start(),
                payload,
                0,
                payload.length);

        return new RetrievalRecord(
                Long.toUnsignedString(actualAgentId),
                timestamp,
                eventType,
                Base64.getEncoder().encodeToString(payload));
    }

    private void verifyChecksum(
            byte[] message,
            long expected,
            long blockId) throws IOException {

        CRC32 crc = new CRC32();
        crc.update(message);

        if (crc.getValue() != expected) {
            throw new IOException(
                    "Record checksum mismatch in block " + blockId);
        }
    }

    private static void readFully(
            FileChannel channel,
            ByteBuffer buffer,
            long position) throws IOException {

        while (buffer.hasRemaining()) {
            int n = channel.read(buffer, position);

            if (n < 0) {
                throw new EOFException("Incomplete segment mapping");
            }

            if (n == 0) {
                Thread.onSpinWait();
                continue;
            }

            position += n;
        }
    }
}
