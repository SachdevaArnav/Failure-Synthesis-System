package Ingestion;

import java.io.IOException;
import java.nio.ByteBuffer;

import Storage.EnvelopeConfig;
import Storage.TelemetryBuffer;

public final class TelemetryIngestor {
    private final TelemetryBuffer buffer;
    private final EnvelopeConfig config;
    private final int maxMessageSize;

    public TelemetryIngestor(
            TelemetryBuffer buffer,
            EnvelopeConfig config,
            int maxMessageSize) throws IOException {

        this.buffer = buffer;
        this.config = config;
        this.maxMessageSize = maxMessageSize;

        config.validateAgainstMaxMessageSize(maxMessageSize);
    }

    public IngestionResult ingest(byte[] message) {
        if (message == null || message.length == 0) {
            return IngestionResult.INVALID;
        }

        if (message.length > maxMessageSize) {
            return IngestionResult.TOO_LARGE;
        }

        if (!isValid(message)) {
            return IngestionResult.INVALID;
        }

        try {
            buffer.offer(ByteBuffer.wrap(message));
            return IngestionResult.ACCEPTED;
        } catch (IOException e) {
            return IngestionResult.BUFFER_REJECTED;
        }
    }

    private boolean isValid(byte[] message) {
        EnvelopeConfig.Field agentId = config.field("Agent ID");
        EnvelopeConfig.Field timestamp = config.field("Timestamp");
        EnvelopeConfig.Field payloadLength = config.field("Payload Length");
        EnvelopeConfig.Field eventType = config.field("Event Type");
        EnvelopeConfig.Field payload = config.field("Payload");

        if (agentId.end() >= message.length
                || timestamp.end() >= message.length
                || payloadLength.end() >= message.length
                || eventType.end() >= message.length) {
            return false;
        }

        int payloadStart = payload.start();

        if (payloadStart > message.length) {
            return false;
        }

        long declaredLength = Integer.toUnsignedLong(
                ByteBuffer.wrap(message)
                        .order(config.byteOrder())
                        .getInt(payloadLength.start()));

        return declaredLength == message.length - payloadStart;
    }
}
