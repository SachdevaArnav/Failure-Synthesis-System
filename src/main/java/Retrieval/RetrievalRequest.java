package Retrieval;

import com.fasterxml.jackson.databind.JsonNode;

public record RetrievalRequest(
        boolean allSources,
        long agentId,
        long startTime,
        long endTime,
        Long eventType) {

    public static RetrievalRequest fromJson(JsonNode json) {
        if (json == null || !json.isObject()) {
            throw new IllegalArgumentException("Request must be a JSON object");
        }

        JsonNode source = json.get("source");
        JsonNode start = json.get("start_time");
        JsonNode end = json.get("end_time");
        JsonNode event = json.get("event_type");

        if (source == null || start == null || end == null) {
            throw new IllegalArgumentException("Missing required request fields");
        }

        if (!start.isIntegralNumber() || !end.isIntegralNumber()) {
            throw new IllegalArgumentException("Times must be integers");
        }

        long startTime = start.longValue();
        long endTime = end.longValue();

        if (startTime > endTime) {
            throw new IllegalArgumentException("Invalid time range");
        }

        boolean allSources;
        long agentId = 0;

        if (source.isTextual() && source.asText().equals("all")) {
            allSources = true;
        } else if (source.isIntegralNumber()) {
            allSources = false;
            agentId = source.longValue();
        } else if (source.isTextual()) {
            allSources = false;
            try {
                agentId = Long.parseUnsignedLong(source.asText());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid agent ID");
            }
        } else {
            throw new IllegalArgumentException(
                    "Source must be 'all' or an agent ID");
        }

        if (event != null && !event.isNull() && !event.isIntegralNumber()) {
            throw new IllegalArgumentException("event_type must be an integer or null");
        }

        Long eventType = event == null || event.isNull()
                ? null
                : event.longValue();

        return new RetrievalRequest(
                allSources, agentId, startTime, endTime, eventType);
    }
}
