
package Ingestion;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

public final class MqttTransportAdapter implements TransportAdapter {

    private final Mqtt5AsyncClient client;
    private final String topicFilter;
    private final String clientId;
    private final TelemetryIngestor ingestor;
    private static final AtomicInteger activeCallbacks = new AtomicInteger();
    private static final AtomicInteger maxConcurrentCallbacks = new AtomicInteger();
    private final AtomicInteger received = new AtomicInteger();
    private final AtomicInteger accepted = new AtomicInteger();
    private final AtomicInteger rejected = new AtomicInteger();

    private final ExecutorService callbackExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public MqttTransportAdapter(
            String host,
            int port,
            String clientId,
            String topicFilter,
            TelemetryIngestor ingestor) {

        this.client = Mqtt5Client.builder()
                .identifier(clientId)
                .serverHost(host)
                .serverPort(port)
                .buildAsync();

        this.clientId = clientId;
        this.topicFilter = topicFilter;
        this.ingestor = ingestor;
    }

    @Override
    public void start() {
        client.connect().join();

        client.subscribeWith()
                .topicFilter(topicFilter)
                .qos(MqttQos.AT_MOST_ONCE)
                .callback(publish -> {
                    received.incrementAndGet();

                    int active = activeCallbacks.incrementAndGet();
                    maxConcurrentCallbacks.accumulateAndGet(active, Math::max);

                    try {
                        IngestionResult result = ingestor.ingest(publish.getPayloadAsBytes());

                        if (result == IngestionResult.ACCEPTED) {
                            accepted.incrementAndGet();
                        } else {
                            rejected.incrementAndGet();
                        }
                    } finally {
                        activeCallbacks.decrementAndGet();
                    }
                })
                .executor(callbackExecutor)
                .send()
                .join();
    }

    public void printStats() {
        System.out.println(
                clientId +
                        " | Received: " + received.get() +
                        " | Accepted: " + accepted.get() +
                        " | Rejected: " + rejected.get());
        System.out.println(
                "Maximum concurrent callbacks: " + maxConcurrentCallbacks.get());
    }

    @Override
    public void close() {
        client.disconnect().join();
        callbackExecutor.shutdown();
    }
}
