
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import Ingestion.IngestionResult;
import Ingestion.TelemetryIngestor;
import Retrieval.RetrievalRecord;
import Retrieval.RetrievalService;
import Storage.BlockAllocator;
import Storage.BlockMetadataStore;
import Storage.BlockStore;
import Storage.EnvelopeConfig;
import Storage.SegmentManager;
import Storage.SegmentMetadataStore;
import Storage.StorageConfig;
import Storage.StorageWorker;
import Storage.TelemetryBuffer;

public final class DirectIngestionRetrievalTest {

        private static final int AGENTS = 3;
        private static final int MESSAGES_PER_AGENT = 5;

        private record ExpectedRecord(
                        String agentId,
                        long timestamp,
                        long eventType,
                        String payload) {
        }

        public static void main(String[] args) throws Exception {

                Path originalProperties = Path.of(
                                args.length > 0 ? args[0] : "storage.properties")
                                .toAbsolutePath();

                Path propertiesPath = Path.of(
                                args.length > 0 ? args[0] : "storage.properties")
                                .toAbsolutePath();

                StorageConfig config = StorageConfig.load(propertiesPath);

                // Clean existing test storage for a fresh run.
                if (Files.exists(config.root)) {
                        try (var paths = Files.walk(config.root)) {
                                paths.sorted(java.util.Comparator.reverseOrder())
                                                .forEach(path -> {
                                                        try {
                                                                Files.delete(path);
                                                        } catch (Exception e) {
                                                                throw new RuntimeException(
                                                                                "Failed to clean storage: " + path, e);
                                                        }
                                                });
                        }
                }

                Files.createDirectories(config.root);

                System.out.println("Test storage: " + config.root);
                BlockStore blockStore = null;
                BlockMetadataStore metadataStore = null;
                TelemetryBuffer buffer = null;

                AtomicInteger accepted = new AtomicInteger();

                AtomicReference<Throwable> workerFailure = new AtomicReference<>();

                List<ExpectedRecord> expectedRecords = new ArrayList<>();

                long baseTimestamp = System.currentTimeMillis();

                try {

                        blockStore = new BlockStore(
                                        config.root.resolve("blocks.dat"));

                        metadataStore = new BlockMetadataStore(
                                        config.root.resolve("block-metadata.dat"),
                                        config.blockSize);

                        SegmentMetadataStore segmentStore = new SegmentMetadataStore(config.root);

                        BlockAllocator allocator = new BlockAllocator(
                                        config.blockSize,
                                        metadataStore);

                        SegmentManager segmentManager = new SegmentManager(
                                        config,
                                        blockStore,
                                        metadataStore,
                                        allocator,
                                        segmentStore);

                        buffer = new TelemetryBuffer(
                                        config.bufferCapacityBytes);

                        StorageWorker worker = new StorageWorker(
                                        config,
                                        buffer,
                                        segmentManager);

                        TelemetryBuffer finalBuffer = buffer;

                        Thread storageThread = Thread.ofPlatform()
                                        .name("retrieval-test-storage")
                                        .start(() -> {
                                                try {
                                                        worker.processUntilClosed();
                                                } catch (Throwable e) {
                                                        workerFailure.set(e);
                                                        finalBuffer.close();
                                                }
                                        });

                        TelemetryIngestor ingestor = new TelemetryIngestor(
                                        buffer,
                                        config.envelope,
                                        config.maxMessageSize);

                        System.out.println("\n========== INGESTION ==========");

                        for (int agent = 1; agent <= AGENTS; agent++) {

                                for (int sequence = 1; sequence <= MESSAGES_PER_AGENT; sequence++) {

                                        long timestamp = baseTimestamp
                                                        + (agent * 1000L)
                                                        + sequence;

                                        long eventType = sequence % 2 == 0 ? 2 : 1;

                                        String payload = "agent-" + agent
                                                        + "-message-" + sequence;

                                        byte[] message = createMessage(
                                                        config.envelope,
                                                        agent,
                                                        timestamp,
                                                        eventType,
                                                        payload);

                                        IngestionResult result = ingestor.ingest(message);

                                        if (result != IngestionResult.ACCEPTED) {
                                                throw new AssertionError(
                                                                "Ingestion rejected: agent="
                                                                                + agent
                                                                                + ", sequence="
                                                                                + sequence
                                                                                + ", result="
                                                                                + result);
                                        }

                                        accepted.incrementAndGet();

                                        expectedRecords.add(new ExpectedRecord(
                                                        Integer.toString(agent),
                                                        timestamp,
                                                        eventType,
                                                        payload));

                                        System.out.println(
                                                        "ACCEPTED | agent=" + agent
                                                                        + " | timestamp=" + timestamp
                                                                        + " | event=" + eventType
                                                                        + " | payload=" + payload);
                                }
                        }

                        // Close buffer so worker drains all remaining messages.
                        buffer.close();

                        storageThread.join();

                        if (workerFailure.get() != null) {
                                throw new RuntimeException(
                                                "Storage worker failed",
                                                workerFailure.get());
                        }

                        System.out.println("\nIngestion completed.");
                        System.out.println("Accepted: " + accepted.get());

                        // ==================================================
                        // RETRIEVAL TEST
                        // ==================================================

                        System.out.println("\n========== RETRIEVAL ==========");

                        RetrievalService retrievalService = new RetrievalService(
                                        config,
                                        blockStore,
                                        metadataStore,
                                        segmentStore);

                        long startTime = baseTimestamp;
                        long endTime = baseTimestamp + 10000;

                        List<RetrievalRecord> retrieved = retrievalService.retrieve(
                                        true, // All agents
                                        0L, // Ignored when allSources=true
                                        startTime,
                                        endTime,
                                        null); // No event-type filter

                        System.out.println(
                                        "Expected records: " + expectedRecords.size());

                        System.out.println(
                                        "Retrieved records: " + retrieved.size());

                        // ==================================================
                        // VALIDATION
                        // ==================================================

                        Map<String, ExpectedRecord> expectedMap = new HashMap<>();

                        for (ExpectedRecord record : expectedRecords) {
                                expectedMap.put(
                                                record.agentId() + ":" + record.timestamp(),
                                                record);
                        }

                        int matched = 0;

                        for (RetrievalRecord actual : retrieved) {

                                String key = actual.agentId()
                                                + ":"
                                                + actual.timestamp();

                                ExpectedRecord expected = expectedMap.get(key);

                                if (expected == null) {
                                        throw new AssertionError(
                                                        "Unexpected retrieved record: " + key);
                                }

                                String decodedPayload = new String(
                                                Base64.getDecoder().decode(actual.payload()),
                                                StandardCharsets.UTF_8);

                                if (actual.eventType() != expected.eventType()) {
                                        throw new AssertionError(
                                                        "Event type mismatch for " + key
                                                                        + " | expected="
                                                                        + expected.eventType()
                                                                        + " | actual="
                                                                        + actual.eventType());
                                }

                                if (!decodedPayload.equals(expected.payload())) {
                                        throw new AssertionError(
                                                        "Payload mismatch for " + key
                                                                        + " | expected="
                                                                        + expected.payload()
                                                                        + " | actual="
                                                                        + decodedPayload);
                                }

                                expectedMap.remove(key);
                                matched++;

                                System.out.println(
                                                "MATCHED | agent=" + actual.agentId()
                                                                + " | timestamp=" + actual.timestamp()
                                                                + " | event=" + actual.eventType()
                                                                + " | payload=" + decodedPayload);
                        }

                        if (!expectedMap.isEmpty()) {
                                throw new AssertionError(
                                                "Missing records: " + expectedMap.values());
                        }

                        if (matched != AGENTS * MESSAGES_PER_AGENT) {
                                throw new AssertionError(
                                                "Incorrect retrieval count: " + matched);
                        }

                        System.out.println("\n========== TEST SUMMARY ==========");
                        System.out.println("Agents: " + AGENTS);
                        System.out.println("Messages per agent: "
                                        + MESSAGES_PER_AGENT);
                        System.out.println("Accepted: " + accepted.get());
                        System.out.println("Retrieved: " + retrieved.size());
                        System.out.println("Matched: " + matched);
                        System.out.println("Payload validation: PASSED");
                        System.out.println("Event type validation: PASSED");
                        System.out.println("Timestamp validation: PASSED");
                        System.out.println("\nTEST PASSED");

                } finally {

                        if (buffer != null) {
                                buffer.close();
                        }

                        if (metadataStore != null) {
                                metadataStore.close();
                        }

                        if (blockStore != null) {
                                blockStore.close();
                        }

                }
        }

        private static byte[] createMessage(
                        EnvelopeConfig config,
                        long agentId,
                        long timestamp,
                        long eventType,
                        String payload) {

                byte[] payloadBytes = payload.getBytes(
                                StandardCharsets.UTF_8);

                int payloadStart = config.field("Payload").start();

                byte[] message = new byte[payloadStart + payloadBytes.length];

                ByteBuffer buffer = ByteBuffer.wrap(message)
                                .order(config.byteOrder());

                EnvelopeConfig.Field agent = config.field("Agent ID");

                if (agent.type() == EnvelopeConfig.Type.UINT32) {

                        buffer.putInt(
                                        agent.start(),
                                        (int) agentId);

                } else if (agent.type() == EnvelopeConfig.Type.UINT64) {

                        buffer.putLong(
                                        agent.start(),
                                        agentId);

                } else {
                        throw new IllegalArgumentException(
                                        "Unsupported Agent ID type: " + agent.type());
                }

                buffer.putLong(
                                config.field("Timestamp").start(),
                                timestamp);

                buffer.putInt(
                                config.field("Payload Length").start(),
                                payloadBytes.length);

                buffer.putInt(
                                config.field("Event Type").start(),
                                (int) eventType);

                System.arraycopy(
                                payloadBytes,
                                0,
                                message,
                                payloadStart,
                                payloadBytes.length);

                return message;
        }

        private DirectIngestionRetrievalTest() {
        }
}
