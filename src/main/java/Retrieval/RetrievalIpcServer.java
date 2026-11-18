
package Retrieval;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class RetrievalIpcServer {

    private static final int MAX_FRAME_SIZE = 64 * 1024 * 1024;

    private final RetrievalService retrievalService;
    private final ObjectMapper mapper;

    public RetrievalIpcServer(RetrievalService retrievalService) {
        this.retrievalService = retrievalService;
        this.mapper = new ObjectMapper();
    }

    public void run() throws IOException {

        DataInputStream input = new DataInputStream(System.in);

        DataOutputStream output = new DataOutputStream(System.out);

        while (true) {

            final int requestLength;

            try {
                requestLength = input.readInt();
            } catch (EOFException e) {
                // Python closed its input stream.
                return;
            }

            if (requestLength <= 0 || requestLength > MAX_FRAME_SIZE) {
                throw new IOException(
                        "Invalid IPC request length: " + requestLength);
            }

            byte[] requestBytes = new byte[requestLength];
            input.readFully(requestBytes);

            byte[] responseBytes;

            try {
                JsonNode requestJson = mapper.readTree(requestBytes);

                RetrievalRequest request = RetrievalRequest.fromJson(requestJson);

                RetrievalResponse response = RetrievalResponse.success(
                        retrievalService.retrieve(
                                request.allSources(),
                                request.agentId(),
                                request.startTime(),
                                request.endTime(),
                                request.eventType()));

                responseBytes = mapper.writeValueAsBytes(response);

            } catch (Exception e) {

                System.err.println(
                        "Retrieval request failed: " + e.getMessage());

                RetrievalResponse response = RetrievalResponse.failure(e.getMessage());

                responseBytes = mapper.writeValueAsBytes(response);
            }

            // 4-byte big-endian length followed by JSON response.
            output.writeInt(responseBytes.length);
            output.write(responseBytes);
            output.flush();
        }
    }
}
