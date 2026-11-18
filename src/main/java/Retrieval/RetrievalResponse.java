package Retrieval;

import java.util.List;

public record RetrievalResponse(
        String status,
        List<RetrievalRecord> records,
        String error) {

    public static RetrievalResponse success(List<RetrievalRecord> records) {
        return new RetrievalResponse("OK", records, null);
    }

    public static RetrievalResponse failure(String error) {
        return new RetrievalResponse("ERROR", List.of(), error);
    }
}
