package Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class TelemetryBuffer implements AutoCloseable {
    private static final int WRITING = 0;
    private static final int PUBLISHED = 1;

    private static final class Reservation {
        final long start;
        final int length;
        volatile int state = WRITING;

        Reservation(long start, int length) {
            this.start = start;
            this.length = length;
        }
    }

    private final byte[] data;
    private final int capacity;

    private final AtomicLong availablePos = new AtomicLong();
    private final AtomicLong publishedPos = new AtomicLong();
    private final AtomicLong readPos = new AtomicLong();
    private final AtomicInteger activeProducers = new AtomicInteger();

    private final ConcurrentHashMap<Long, Reservation> reservations = new ConcurrentHashMap<>();

    private volatile boolean accepting = true;

    public TelemetryBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.capacity = capacity;
        this.data = new byte[capacity];
    }

    public void offer(ByteBuffer message) throws IOException {
        if (message == null) {
            throw new NullPointerException("message");
        }

        int length = message.remaining();
        if (length == 0 || length > capacity) {
            throw new IOException("Message length must be between 1 and buffer capacity");
        }

        if (!accepting) {
            throw new IOException("Buffer is closed");
        }

        activeProducers.incrementAndGet();
        try {
            if (!accepting) {
                throw new IOException("Buffer is closed");
            }

            long start = reserve(length);
            Reservation reservation = new Reservation(start, length);
            reservations.put(start, reservation);

            copyIntoRing(message, start, length);

            // Volatile publication makes the preceding byte writes visible.
            reservation.state = PUBLISHED;
            advancePublished();
        } finally {
            activeProducers.decrementAndGet();
        }
    }

    private long reserve(int length) throws IOException {
        for (;;) {
            if (!accepting) {
                throw new IOException("Buffer is closed");
            }

            long start = availablePos.get();
            long consumed = readPos.get();

            if (start > Long.MAX_VALUE - length) {
                throw new IOException("Buffer position exhausted");
            }

            if (start + length - consumed > capacity) {
                LockSupport.parkNanos(100_000);
                continue;
            }

            if (availablePos.compareAndSet(start, start + length)) {
                return start;
            }
        }
    }

    private void copyIntoRing(ByteBuffer source, long start, int length) {
        int index = (int) (start % capacity);
        int first = Math.min(length, capacity - index);

        source.get(data, index, first);
        if (first < length) {
            source.get(data, 0, length - first);
        }
    }

    private void advancePublished() {
        for (;;) {
            long current = publishedPos.get();
            Reservation reservation = reservations.get(current);

            if (reservation == null || reservation.state != PUBLISHED) {
                return;
            }

            long next = current + reservation.length;
            if (!publishedPos.compareAndSet(current, next)) {
                continue;
            }
        }
    }

    /**
     * Copies one complete record into dst and flips dst for reading.
     * Returns -1 when the buffer is closed and fully drained.
     */
    public int take(ByteBuffer dst) throws IOException {
        for (;;) {
            long current = readPos.get();
            long published = publishedPos.get();

            if (current < published) {
                Reservation reservation = reservations.get(current);
                if (reservation == null || reservation.state != PUBLISHED) {
                    throw new IOException("Missing published reservation at " + current);
                }
                if (dst.capacity() < reservation.length) {
                    throw new IOException("Destination buffer is too small");
                }

                dst.clear();
                copyFromRing(dst, current, reservation.length);
                dst.flip();

                reservations.remove(current, reservation);
                readPos.set(current + reservation.length);
                return reservation.length;
            }

            if (!accepting
                    && activeProducers.get() == 0
                    && readPos.get() == availablePos.get()) {
                return -1;
            }

            LockSupport.parkNanos(100_000);
        }
    }

    private void copyFromRing(ByteBuffer dst, long start, int length) {
        int index = (int) (start % capacity);
        int first = Math.min(length, capacity - index);

        dst.put(data, index, first);
        if (first < length) {
            dst.put(data, 0, length - first);
        }
    }

    @Override
    public void close() {
        accepting = false;
    }
}
