package com.recoveryguard.lab.groundtruth;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class WorkloadProducer {
    private static final int INTENT_BATCH_SIZE = 128;
    private static final Duration PRODUCER_DEADLINE = Duration.ofSeconds(300);

    int produce(RunStore.Run run, int count, int payloadBytes, long intervalMs) throws Exception {
        if (count < 1 || count > 1_000_000) {
            throw new IllegalArgumentException("count must be between 1 and 1000000");
        }
        if (payloadBytes < 1 || payloadBytes > 1_000_000) {
            throw new IllegalArgumentException("payloadBytes must be between 1 and 1000000");
        }
        if (intervalMs < 0 || intervalMs > 60_000) {
            throw new IllegalArgumentException("intervalMs must be between 0 and 60000");
        }

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, run.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "recoveryguard-lab-" + run.runId());

        CountDownLatch callbacks = new CountDownLatch(count);
        AtomicInteger confirmed = new AtomicInteger();
        AtomicReference<IOException> ledgerFailure = new AtomicReference<>();
        DurableEventWriter eventWriter = new DurableEventWriter(run.ledger());
        KafkaProducer<String, String> producer = null;
        Exception primaryFailure = null;
        long deadline = 0;
        int sendAttempts = 0;

        try {
            producer = new KafkaProducer<>(properties);
            deadline = System.nanoTime() + PRODUCER_DEADLINE.toNanos();

            List<PendingSend> batch = new ArrayList<>(INTENT_BATCH_SIZE);
            for (int index = 0; index < count; index++) {
                ensureBeforeDeadline(deadline, "Timed out while creating the workload");

                String recordId = run.runId() + "-" + UUID.randomUUID();
                String payload = buildPayload(recordId, payloadBytes);
                String payloadHash = Hashing.sha256(payload);
                batch.add(new PendingSend(recordId, payload, payloadHash));

                if (batch.size() == INTENT_BATCH_SIZE || index == count - 1) {
                    List<EventLedger.Entry> intents = batch.stream()
                            .map(pending -> new EventLedger.Entry("SEND_INTENT", Map.of(
                                    "recordId", pending.recordId(),
                                    "topic", run.topic(),
                                    "key", pending.recordId(),
                                    "payloadSha256", pending.payloadHash(),
                                    "payloadBytes", pending.payload().getBytes(StandardCharsets.UTF_8).length
                            )))
                            .toList();

                    // The durable intent batch must exist before Kafka receives any record in this batch.
                    run.ledger().appendBatch(intents);

                    for (PendingSend pending : batch) {
                        ensureBeforeDeadline(deadline, "Timed out before all workload records were sent");
                        if (sendAttempts > 0 && intervalMs > 0) {
                            sleepBeforeDeadline(intervalMs, deadline);
                        }
                        sendAttempts++;

                        ProducerRecord<String, String> record =
                                new ProducerRecord<>(run.topic(), pending.recordId(), pending.payload());
                        try {
                            producer.send(record, (metadata, exception) -> {
                                try {
                                    if (exception == null && metadata != null) {
                                        eventWriter.append("ACK_CONFIRMED", Map.of(
                                                "recordId", pending.recordId(),
                                                "topic", metadata.topic(),
                                                "partition", metadata.partition(),
                                                "offset", metadata.offset(),
                                                "timestamp", metadata.timestamp(),
                                                "payloadSha256", pending.payloadHash()
                                        ));
                                        confirmed.incrementAndGet();
                                    } else {
                                        eventWriter.append("OUTCOME_UNKNOWN", Map.of(
                                                "recordId", pending.recordId(),
                                                "topic", run.topic(),
                                                "payloadSha256", pending.payloadHash(),
                                                "errorType", exception == null
                                                        ? "MissingRecordMetadata" : exception.getClass().getName(),
                                                "errorMessage", exception == null
                                                        ? "Producer callback returned no metadata" : safeMessage(exception)
                                        ));
                                    }
                                } catch (IOException ioException) {
                                    ledgerFailure.compareAndSet(null, ioException);
                                } finally {
                                    callbacks.countDown();
                                }
                            });
                        } catch (RuntimeException sendException) {
                            try {
                                eventWriter.append("OUTCOME_UNKNOWN", Map.of(
                                        "recordId", pending.recordId(),
                                        "topic", run.topic(),
                                        "payloadSha256", pending.payloadHash(),
                                        "errorType", sendException.getClass().getName(),
                                        "errorMessage", safeMessage(sendException)
                                ));
                            } catch (IOException ioException) {
                                ledgerFailure.compareAndSet(null, ioException);
                            } finally {
                                callbacks.countDown();
                            }
                        }
                    }
                    batch.clear();
                }
            }

            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0
                    || !callbacks.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                throw new IOException("Timed out waiting for producer callbacks. Unfinished intents remain unresolved.");
            }
        } catch (Exception exception) {
            primaryFailure = exception;
        } finally {
            if (producer != null) {
                try {
                    producer.close(remainingDuration(deadline));
                } catch (RuntimeException closeException) {
                    primaryFailure = appendFailure(primaryFailure,
                            new IOException("Could not close Kafka producer cleanly", closeException));
                }
            }

            try {
                eventWriter.close(remainingDuration(deadline));
            } catch (IOException closeException) {
                primaryFailure = appendFailure(primaryFailure, closeException);
            }
        }

        if (primaryFailure != null) {
            throw primaryFailure;
        }
        if (ledgerFailure.get() != null) {
            throw new IOException("Could not persist a producer result to the external ledger", ledgerFailure.get());
        }

        run.ledger().append("PRODUCE_BATCH_COMPLETED", Map.of(
                "requestedCount", count,
                "confirmedCount", confirmed.get(),
                "completedAt", java.time.Instant.now().toString()
        ));
        return confirmed.get();
    }

    private static Duration remainingDuration(long deadline) {
        if (deadline == 0) {
            return Duration.ZERO;
        }
        return Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
    }

    private static void ensureBeforeDeadline(long deadline, String message) throws IOException {
        if (System.nanoTime() >= deadline) {
            throw new IOException(message);
        }
    }

    private static void sleepBeforeDeadline(long intervalMs, long deadline) throws IOException {
        long remainingNanos = deadline - System.nanoTime();
        if (remainingNanos <= 0) {
            throw new IOException("Timed out while pacing the workload");
        }
        long requestedNanos = TimeUnit.MILLISECONDS.toNanos(intervalMs);
        if (requestedNanos >= remainingNanos) {
            throw new IOException("Workload pacing interval exceeds the remaining deadline");
        }
        try {
            TimeUnit.NANOSECONDS.sleep(requestedNanos);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IOException("Workload production was interrupted", interruptedException);
        }
    }

    private static Exception appendFailure(Exception primary, Exception additional) {
        if (primary == null) {
            return additional;
        }
        primary.addSuppressed(additional);
        return primary;
    }

    private static String buildPayload(String recordId, int totalBytes) {
        String prefix = "recoveryguard-lab|" + recordId + "|";
        StringBuilder builder = new StringBuilder(prefix);
        while (builder.length() < totalBytes) {
            builder.append('x');
        }
        if (builder.length() > totalBytes) {
            return builder.substring(0, totalBytes);
        }
        return builder.toString();
    }

    private static String safeMessage(Throwable throwable) {
        String message = throwable.getMessage();
        if (message == null) {
            return "";
        }
        return message.length() > 1000 ? message.substring(0, 1000) : message;
    }

    private record PendingSend(String recordId, String payload, String payloadHash) {
    }
}
