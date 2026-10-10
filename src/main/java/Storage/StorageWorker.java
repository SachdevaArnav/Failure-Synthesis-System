package Storage;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class StorageWorker {
    private final StorageConfig config;
    private final TelemetryBuffer buffer;
    private final SegmentManager segmentManager;

    public StorageWorker(
            StorageConfig config,
            TelemetryBuffer buffer,
            SegmentManager segmentManager) {
        this.config = config;
        this.buffer = buffer;
        this.segmentManager = segmentManager;
    }

    public void processUntilClosed() throws IOException {
        ByteBuffer message = ByteBuffer.allocate(config.bufferCapacityBytes);

        for (;;) {
            int length = buffer.take(message);
            if (length == -1) {
                return;
            }

            if (length > config.maxMessageSize) {
                throw new IOException("Incoming message exceeds maxMessageSize");
            }

            message.order(config.envelope.byteOrder());

            long agentId = readAgentId(message);
            long timestamp = message.getLong(
                    config.envelope.field("Timestamp").start());

            long payloadLength = Integer.toUnsignedLong(
                    message.getInt(config.envelope.field("Payload Length").start()));

            // Read the field so malformed or unsupported event layouts are
            // detected by the same configured-envelope contract.
            EnvelopeConfig.Field eventType = config.envelope.field("Event Type");
            if (eventType.type() == EnvelopeConfig.Type.UINT32) {
                Integer.toUnsignedLong(message.getInt(eventType.start()));
            } else {
                message.getInt(eventType.start());
            }

            int payloadStart = config.envelope.field("Payload").start();
            if (payloadLength > Integer.MAX_VALUE
                    || (long) payloadStart + payloadLength != length) {
                throw new IOException("Payload length does not match message boundary");
            }

            message.position(0);
            message.limit(length);

            segmentManager.append(agentId, timestamp, message);
        }
    }

    private long readAgentId(ByteBuffer message) throws IOException {
        EnvelopeConfig.Field field = config.envelope.field("Agent ID");

        return switch (field.type()) {
            case UINT32 ->
                Integer.toUnsignedLong(message.getInt(field.start()));
            case UINT64 ->
                message.getLong(field.start());
            default ->
                throw new IOException("Unsupported Agent ID type");
        };
    }
}
