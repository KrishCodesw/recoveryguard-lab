package com.recoveryguard.lab.groundtruth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

final class DurableEventWriter implements AutoCloseable {
    private static final int MAX_BATCH_SIZE = 256;
    private static final EventLedger.Entry POISON = new EventLedger.Entry("__STOP__", Map.of());

    private final EventLedger ledger;
    private final BlockingQueue<EventLedger.Entry> queue = new ArrayBlockingQueue<>(8192);
    private final AtomicReference<IOException> failure = new AtomicReference<>();
    private final Thread worker;
    private volatile boolean closed;

    DurableEventWriter(EventLedger ledger) {
        this.ledger = ledger;
        this.worker = new Thread(this::run, "ground-truth-ledger-writer");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    void append(String eventType, Map<String, ?> fields) throws IOException {
        IOException existingFailure = failure.get();
        if (existingFailure != null) {
            throw new IOException("Ledger writer has failed", existingFailure);
        }
        if (closed) {
            throw new IOException("Ledger writer is closed");
        }
        try {
            EventLedger.Entry entry = new EventLedger.Entry(eventType, fields);
            while (!queue.offer(entry, 100, TimeUnit.MILLISECONDS)) {
                IOException writerFailure = failure.get();
                if (writerFailure != null) {
                    throw new IOException("Ledger writer has failed", writerFailure);
                }
                if (closed || !worker.isAlive()) {
                    throw new IOException("Ledger writer is not accepting events");
                }
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while queuing a ledger event", interruptedException);
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
        if (closed) {
            IOException existingFailure = failure.get();
            if (existingFailure != null) {
                throw new IOException("Ledger writer failed", existingFailure);
            }
            return;
        }
        closed = true;
        try {
            while (worker.isAlive() && !queue.offer(POISON, 100, TimeUnit.MILLISECONDS)) {
                if (failure.get() != null && !worker.isAlive()) {
                    break;
                }
            }
            worker.join();
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing the ledger writer", interruptedException);
        }

        IOException existingFailure = failure.get();
        if (existingFailure != null) {
            throw new IOException("Ledger writer failed", existingFailure);
        }
    }
}
