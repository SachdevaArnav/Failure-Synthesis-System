package Storage;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class EnvelopeConfig {
    public enum Type {
        UINT32, UINT64, INT32, INT64, BYTES
    }

    public record Field(String name, Type type, int start, int end) {
        public int width() {
            if (end < start) {
                throw new IllegalStateException("Variable-width field");
            }
            return end - start + 1;
        }
    }

    private final Map<String, Field> fields;
    private final ByteOrder byteOrder;

    private EnvelopeConfig(Map<String, Field> fields, ByteOrder byteOrder) {
        this.fields = Map.copyOf(fields);
        this.byteOrder = byteOrder;
    }

    public static EnvelopeConfig load(Path path, ByteOrder byteOrder)
            throws IOException {
        Map<String, Field> fields = new HashMap<>();
        List<String> lines = Files.readAllLines(path);

        for (int lineNo = 1; lineNo <= lines.size(); lineNo++) {
            String line = lines.get(lineNo - 1).trim();

            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            String[] parts = line.split(",", -1);
            if (parts.length != 3) {
                throw new IOException("Invalid envelope.cfg line " + lineNo);
            }

            String[] range = parts[0].trim().split("-", -1);
            if (range.length != 2) {
                throw new IOException("Invalid byte range at line " + lineNo);
            }

            int start;
            int end;
            try {
                start = Integer.parseInt(range[0].trim());
                end = range[1].trim().equals("*")
                        ? -1
                        : Integer.parseInt(range[1].trim());
            } catch (NumberFormatException e) {
                throw new IOException("Invalid byte range at line " + lineNo, e);
            }

            String name = parts[1].trim();
            Type type;
            try {
                type = Type.valueOf(parts[2].trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IOException("Unknown field type at line " + lineNo, e);
            }

            if (start < 0 || (end != -1 && end < start)) {
                throw new IOException("Invalid range at line " + lineNo);
            }
            if (end == -1 && type != Type.BYTES) {
                throw new IOException("Only BYTES may use an open-ended range");
            }
            if (type != Type.BYTES && end - start + 1 != expectedWidth(type)) {
                throw new IOException("Width does not match type at line " + lineNo);
            }
            if (fields.putIfAbsent(name, new Field(name, type, start, end)) != null) {
                throw new IOException("Duplicate field name: " + name);
            }
        }

        EnvelopeConfig config = new EnvelopeConfig(fields, byteOrder);
        config.validate();
        return config;
    }

    private static int expectedWidth(Type type) {
        return switch (type) {
            case UINT32, INT32 -> 4;
            case UINT64, INT64 -> 8;
            case BYTES -> -1;
        };
    }

    private void validate() throws IOException {
        require("Agent ID", Type.UINT32, Type.UINT64);
        require("Timestamp", Type.INT64);
        require("Payload Length", Type.UINT32);
        require("Event Type", Type.UINT32, Type.INT32);
        require("Payload", Type.BYTES);

        Field payload = fields.get("Payload");
        if (payload.end() != -1) {
            throw new IOException("Payload range must end with '*'");
        }

        for (Field a : fields.values()) {
            if (a.end() == -1) {
                continue;
            }
            for (Field b : fields.values()) {
                if (a == b || b.end() == -1) {
                    continue;
                }
                if (a.start() <= b.end() && b.start() <= a.end()) {
                    throw new IOException(
                            "Overlapping fields: " + a.name() + " and " + b.name());
                }
            }
        }
    }

    private void require(String name, Type... allowed) throws IOException {
        Field field = fields.get(name);
        if (field == null) {
            throw new IOException("Missing required field: " + name);
        }
        for (Type type : allowed) {
            if (field.type() == type) {
                return;
            }
        }
        throw new IOException("Invalid type for field: " + name);
    }

    public Field field(String name) {
        Field field = fields.get(name);
        if (field == null) {
            throw new IllegalArgumentException("Unknown field: " + name);
        }
        return field;
    }

    public ByteOrder byteOrder() {
        return byteOrder;
    }

    public void validateAgainstMaxMessageSize(int maxMessageSize)
            throws IOException {
        int lastHeaderByte = -1;
        Field payload = fields.get("Payload");

        for (Field field : fields.values()) {
            if (field == payload) {
                continue;
            }

            if (field.end() >= maxMessageSize) {
                throw new IOException(
                        "Field exceeds maxMessageSize: " + field.name());
            }

            lastHeaderByte = Math.max(lastHeaderByte, field.end());
        }

        if (payload.start() <= lastHeaderByte
                || payload.start() >= maxMessageSize) {
            throw new IOException("Invalid payload start offset");
        }
    }

}
