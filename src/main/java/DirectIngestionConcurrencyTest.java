
// import java.io.InputStream;
// import java.io.OutputStream;
// import java.nio.ByteBuffer;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.Properties;
// import java.util.concurrent.CountDownLatch;
// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.Executors;
// import java.util.concurrent.Future;
// import java.util.concurrent.TimeUnit;
// import java.util.concurrent.atomic.AtomicLong;
// import java.util.concurrent.atomic.LongAdder;

// import Ingestion.IngestionResult;
// import Ingestion.TelemetryIngestor;
// import Storage.BlockAllocator;
// import Storage.BlockMetadataStore;
// import Storage.BlockStore;
// import Storage.ConcurrencyMetrics;
// import Storage.EnvelopeConfig;
// import Storage.SegmentManager;
// import Storage.SegmentMetadataStore;
// import Storage.StorageConfig;
// import Storage.StorageWorker;
// import Storage.TelemetryBuffer;

// public final class DirectIngestionConcurrencyTest {

// private static final int PRODUCERS = 100;
// private static final int MESSAGES_PER_PRODUCER = 100;
// private static final int MAX_PAYLOAD_SIZE = 4096;

// public static void main(String[] args) throws Exception {

// Path originalProperties = Path.of(
// args.length > 0 ? args[0] : "storage.properties")
// .toAbsolutePath();

// // Create a separate configuration and storage directory.
// Properties properties = new Properties();

// try (InputStream in = Files.newInputStream(originalProperties)) {
// properties.load(in);
// }

// Path testRoot = Files.createTempDirectory(
// "failure-synthesis-concurrency-");

// properties.setProperty(
// "root",
// testRoot.resolve("storage").toString());

// Path testProperties = Files.createTempFile(
// originalProperties.getParent(),
// "storage-concurrency-",
// ".properties");

// try (OutputStream out = Files.newOutputStream(testProperties)) {
// properties.store(out, "Temporary concurrency test configuration");
// }

// StorageConfig config = StorageConfig.load(testProperties);
// Files.createDirectories(config.root);

// System.out.println("Test storage: " + config.root);

// BlockStore blockStore = null;
// BlockMetadataStore metadataStore = null;
// TelemetryBuffer buffer = null;

// AtomicLong accepted = new AtomicLong();
// AtomicLong rejected = new AtomicLong();
// LongAdder unexpected = new LongAdder();

// try {
// blockStore = new BlockStore(
// config.root.resolve("blocks.dat"));

// metadataStore = new BlockMetadataStore(
// config.root.resolve("block-metadata.dat"),
// config.blockSize);

// SegmentMetadataStore segmentStore = new SegmentMetadataStore(config.root);

// BlockAllocator allocator = new BlockAllocator(config.blockSize,
// metadataStore);

// SegmentManager segmentManager = new SegmentManager(
// config,
// blockStore,
// metadataStore,
// allocator,
// segmentStore);

// buffer = new TelemetryBuffer(config.bufferCapacityBytes);

// StorageWorker worker = new StorageWorker(
// config, buffer, segmentManager);

// TelemetryBuffer finalBuffer = buffer;

// AtomicLong workerFailure = new AtomicLong();
// java.util.concurrent.atomic.AtomicReference<Throwable> failure = new
// java.util.concurrent.atomic.AtomicReference<>();

// Thread storageThread = Thread.ofPlatform()
// .name("concurrency-test-storage")
// .start(() -> {
// try {
// worker.processUntilClosed();
// } catch (Throwable e) {
// failure.set(e);
// finalBuffer.close();
// }
// });

// TelemetryIngestor ingestor = new TelemetryIngestor(
// buffer,
// config.envelope,
// config.maxMessageSize);

// int payloadStart = config.envelope.field("Payload").start();

// int payloadSize = Math.min(
// MAX_PAYLOAD_SIZE,
// config.maxMessageSize - payloadStart);

// if (payloadSize <= 0) {
// throw new IllegalStateException(
// "Configured maxMessageSize cannot fit a payload");
// }

// int messageSize = payloadStart + payloadSize;

// System.out.println("Producers: " + PRODUCERS);
// System.out.println("Messages per producer: "
// + MESSAGES_PER_PRODUCER);
// System.out.println("Total messages: "
// + (PRODUCERS * MESSAGES_PER_PRODUCER));
// System.out.println("Message size: " + messageSize);

// CountDownLatch startGate = new CountDownLatch(1);

// ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

// List<Future<?>> futures = new ArrayList<>();

// long baseTimestamp = System.currentTimeMillis();

// for (int producer = 1; producer <= PRODUCERS; producer++) {
// final int producerId = producer;

// futures.add(executor.submit(() -> {
// try {
// startGate.await();

// for (int i = 0; i < MESSAGES_PER_PRODUCER; i++) {

// byte[] message = createMessage(
// config.envelope,
// producerId,
// baseTimestamp + i,
// payloadStart,
// payloadSize);

// IngestionResult result = ingestor.ingest(message);

// if (result == IngestionResult.ACCEPTED) {
// accepted.incrementAndGet();
// } else {
// rejected.incrementAndGet();
// }
// }
// } catch (Exception e) {
// unexpected.increment();
// throw new RuntimeException(e);
// }
// }));
// }

// long start = System.nanoTime();

// startGate.countDown();

// try {
// for (Future<?> future : futures) {
// future.get();
// }
// } finally {
// executor.shutdown();
// executor.awaitTermination(1, TimeUnit.MINUTES);
// }

// long ingestionTime = System.nanoTime() - start;

// // Stop producers and let storage drain everything.
// buffer.close();
// storageThread.join();

// if (failure.get() != null) {
// throw new RuntimeException(
// "Storage worker failed", failure.get());
// }

// long expected = (long) PRODUCERS * MESSAGES_PER_PRODUCER;

// System.out.println("\n========== TEST SUMMARY ==========");
// System.out.println("Expected: " + expected);
// System.out.println("Accepted: " + accepted.get());
// System.out.println("Rejected: " + rejected.get());
// System.out.println("Unexpected producer errors: "
// + unexpected.sum());
// System.out.println("Elapsed ingestion time: "
// + TimeUnit.NANOSECONDS.toMillis(ingestionTime)
// + " ms");

// ConcurrencyMetrics.printResults();

// if (accepted.get() != expected
// || rejected.get() != 0
// || unexpected.sum() != 0) {
// throw new AssertionError(
// "Ingestion test did not accept every message");
// }

// System.out.println("\nTEST PASSED");

// } finally {
// if (buffer != null) {
// buffer.close();
// }

// if (blockStore != null) {
// blockStore.close();
// }

// if (metadataStore != null) {
// metadataStore.close();
// }

// Files.deleteIfExists(testProperties);
// }
// }

// private static byte[] createMessage(
// EnvelopeConfig config,
// int agentId,
// long timestamp,
// int payloadStart,
// int payloadSize) {

// byte[] bytes = new byte[payloadStart + payloadSize];

// ByteBuffer message = ByteBuffer.wrap(bytes)
// .order(config.byteOrder());

// EnvelopeConfig.Field agent = config.field("Agent ID");

// if (agent.type() == EnvelopeConfig.Type.UINT32) {
// message.putInt(agent.start(), agentId);
// } else {
// message.putLong(agent.start(), agentId);
// }

// message.putLong(
// config.field("Timestamp").start(),
// timestamp);

// message.putInt(
// config.field("Payload Length").start(),
// payloadSize);

// message.putInt(
// config.field("Event Type").start(),
// 1);

// for (int i = 0; i < payloadSize; i++) {
// bytes[payloadStart + i] = (byte) (agentId + i);
// }

// return bytes;
// }

// private DirectIngestionConcurrencyTest() {
// }
// }
