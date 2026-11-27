package com.recoveryguard.lab.groundtruth;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

final class DurableEventWriter implements AutoCloseable {
    private static final int MAX_BATCH_SIZE = 256;
    private static final int CAPACITY = 8192;
    private static final EventLedger.Entry POISON = new EventLedger.Entry("__STOP__", Map.of());
    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30);
    private static final long QUEUE_WAIT_MILLIS = 100;

    private final EventLedger ledger;
    private final BlockingQueue<EventLedger.Entry> queue = new ArrayBlockingQueue<>(CAPACITY);
    private final Semaphore capacity = new Semaphore(CAPACITY);
    private final AtomicReference<IOException> failure = new AtomicReference<>();
    private final Object lifecycleLock = new Object();
    private final Thread worker;

    private volatile boolean closed;
    private boolean poisonEnqueued;

    DurableEventWriter(EventLedger ledger) {
        this.ledger = ledger;
        this.worker = new Thread(this::run, "ground-truth-ledger-writer");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    void append(String eventType, Map<String, ?> fields) throws IOException {
        EventLedger.Entry entry = new EventLedger.Entry(eventType, fields);

        try {
            while (!capacity.tryAcquire(QUEUE_WAIT_MILLIS, TimeUnit.MILLISECONDS)) {
                checkAccepting();
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for ledger capacity", interruptedException);
        }

        boolean queued = false;
        try {
            synchronized (lifecycleLock) {
                checkAccepting();
                queue.add(entry);
                queued = true;
            }
        } catch (RuntimeException exception) {
            throw new IOException("Could not enqueue ledger event", exception);
        } finally {
            if (!queued) {
                capacity.release();
            }
        }
    }

    private void checkAccepting() throws IOException {
        IOException existingFailure = failure.get();
        if (existingFailure != null) {
            throw new IOException("Ledger writer has failed", existingFailure);
        }
        if (closed || !worker.isAlive()) {
            throw new IOException("Ledger writer is closed");
        }
    }

    private void run() {
        try {
            boolean stop = false;
            while (!stop) {
                EventLedger.Entry first = queue.take();
                if (first == POISON) {
                    break;
                }

                List<EventLedger.Entry> batch = new ArrayList<>(MAX_BATCH_SIZE);
                batch.add(first);

                EventLedger.Entry next = queue.poll(10, TimeUnit.MILLISECONDS);
                if (next == POISON) {
                    stop = true;
                } else if (next != null) {
                    batch.add(next);
                }

                List<EventLedger.Entry> drained = new ArrayList<>(MAX_BATCH_SIZE);
                queue.drainTo(drained, MAX_BATCH_SIZE - batch.size());
                for (EventLedger.Entry entry : drained) {
                    if (entry == POISON) {
                        stop = true;
                    } else {
                        batch.add(entry);
                    }
                }

                ledger.appendBatch(batch);
                capacity.release(batch.size());
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            failure.compareAndSet(null, new IOException("Ledger writer was interrupted", interruptedException));
        } catch (IOException | RuntimeException exception) {
            IOException ioException = exception instanceof IOException value
                    ? value
                    : new IOException("Ledger writer failed", exception);
            failure.compareAndSet(null, ioException);
        }
    }

    @Override
    public void close() throws IOException {
        close(DEFAULT_CLOSE_TIMEOUT);
    }

    void close(Duration timeout) throws IOException {
        if (timeout == null || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be null or negative");
        }
        long deadline = System.nanoTime() + timeout.toNanos();

        synchronized (lifecycleLock) {
            closed = true;
        }

        while (worker.isAlive() && !isPoisonEnqueued()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new IOException("Timed out while requesting ledger writer shutdown");
            }
            long waitNanos = Math.min(TimeUnit.MILLISECONDS.toNanos(QUEUE_WAIT_MILLIS), remaining);
            try {
                if (!capacity.tryAcquire(waitNanos, TimeUnit.NANOSECONDS)) {
                    checkWorkerFailure();
                    continue;
                }
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while requesting ledger writer shutdown",
                        interruptedException);
            }

            boolean queuedPoison = false;
            synchronized (lifecycleLock) {
                if (!poisonEnqueued && worker.isAlive()) {
                    queue.add(POISON);
                    poisonEnqueued = true;
                    queuedPoison = true;
                }
            }
            if (!queuedPoison) {
                capacity.release();
            }
        }

        try {
            long remaining = deadline - System.nanoTime();
            if (worker.isAlive() && remaining > 0) {
                long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
                int nanos = (int) (remaining - TimeUnit.MILLISECONDS.toNanos(millis));
                worker.join(millis, nanos);
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for the ledger writer", interruptedException);
        }

        if (worker.isAlive()) {
            throw new IOException("Timed out while waiting for the ledger writer to finish");
        }
        checkWorkerFailure();
    }

    private boolean isPoisonEnqueued() {
        synchronized (lifecycleLock) {
            return poisonEnqueued;
        }
    }

    private void checkWorkerFailure() throws IOException {
        IOException existingFailure = failure.get();
        if (existingFailure != null) {
            throw new IOException("Ledger writer has failed", existingFailure);
        }
    }
}
