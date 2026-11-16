package Ingestion;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface MessageHandler {
    void onMessage(String topic, ByteBuffer payload);
}
