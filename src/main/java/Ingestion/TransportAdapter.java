package Ingestion;

public interface TransportAdapter extends AutoCloseable {
    void start() throws Exception;

    @Override
    void close() throws Exception;
}
