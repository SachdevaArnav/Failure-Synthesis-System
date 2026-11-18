package Retrieval;

public record RetrievalRecord(
        String agentId,
        long timestamp,
        long eventType,
        String payload) {
}
