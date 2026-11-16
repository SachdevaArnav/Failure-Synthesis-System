
package Ingestion;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

import Storage.EnvelopeConfig;
import Storage.TelemetryBuffer;

public class TelemetryIngestorTest {

    private EnvelopeConfig loadConfig() throws IOException {
        Path path = Files.createTempFile("envelope", ".cfg");

        Files.writeString(path,
                "0-3,Agent ID,UINT32\n" +
                        "4-11,Timestamp,INT64\n" +
                        "12-15,Payload Length,UINT32\n" +
                        "16-19,Event Type,UINT32\n" +
                        "20-*,Payload,BYTES\n");

        EnvelopeConfig config = EnvelopeConfig.load(path, ByteOrder.BIG_ENDIAN);

        Files.deleteIfExists(path);
        return config;
    }

    private byte[] validMessage() {
        ByteBuffer buffer = ByteBuffer.allocate(23);

        buffer.putInt(1); // Agent ID
        buffer.putLong(1000L); // Timestamp
        buffer.putInt(3); // Payload Length
        buffer.putInt(2); // Event Type
        buffer.put(new byte[] { 10, 20, 30 });

        return buffer.array();
    }

    @Test
    public void acceptsValidMessageAndPreservesBytes() throws Exception {
        TelemetryBuffer buffer = new TelemetryBuffer(128);

        TelemetryIngestor ingestor = new TelemetryIngestor(buffer, loadConfig(), 64);

        byte[] message = validMessage();

        assertEquals(
                IngestionResult.ACCEPTED,
                ingestor.ingest(message));

        ByteBuffer received = ByteBuffer.allocate(64);

        assertEquals(message.length, buffer.take(received));

        byte[] actual = new byte[received.remaining()];
        received.get(actual);

        assertArrayEquals(message, actual);

        buffer.close();
    }

    @Test
    public void rejectsInvalidPayloadLength() throws Exception {
        TelemetryBuffer buffer = new TelemetryBuffer(128);

        TelemetryIngestor ingestor = new TelemetryIngestor(buffer, loadConfig(), 64);

        byte[] message = validMessage();

        // Actual payload is 3 bytes, but declaration says 5.
        ByteBuffer.wrap(message).putInt(12, 5);

        assertEquals(
                IngestionResult.INVALID,
                ingestor.ingest(message));

        buffer.close();
    }

    @Test
    public void rejectsOversizedMessage() throws Exception {
        TelemetryBuffer buffer = new TelemetryBuffer(128);

        TelemetryIngestor ingestor = new TelemetryIngestor(buffer, loadConfig(), 64);

        byte[] message = new byte[65];

        assertEquals(
                IngestionResult.TOO_LARGE,
                ingestor.ingest(message));

        buffer.close();
    }
}
