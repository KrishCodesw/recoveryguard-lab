package com.recoveryguard.lab.groundtruth;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class WorkloadProducer {
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);
    private static final long CALLBACK_TIMEOUT_SECONDS = 300;

    int produce(RunStore.Run run, int count, int payloadBytes, long intervalMs) throws Exception {
        if (count < 1 || count > 1_000_000) {
            throw new IllegalArgumentException("count must be between 1 and 1000000");
        }
        if (payloadBytes < 1 || payloadBytes > 1_000_000) {
            throw new IllegalArgumentException("payloadBytes must be between 1 and 1000000");
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
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "recoveryguard-lab-" + run.runId());

        CountDownLatch callbacks = new CountDownLatch(count);
        AtomicInteger confirmed = new AtomicInteger();
        AtomicReference<IOException> ledgerFailure = new AtomicReference<>();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int index = 0; index < count; index++) {
                String recordId = run.runId() + "-" + UUID.randomUUID();
                String payload = buildPayload(recordId, payloadBytes);
                String payloadHash = Hashing.sha256(payload);

                run.ledger().append("SEND_INTENT", Map.of(
                        "recordId", recordId,
                        "topic", run.topic(),
                        "key", recordId,
                        "payloadSha256", payloadHash,
                        "payloadBytes", payload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                ));

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(run.topic(), recordId, payload);
                try {
                    producer.send(record, (metadata, exception) -> {
                        try {
                            if (exception == null && metadata != null) {
                                run.ledger().append("ACK_CONFIRMED", Map.of(
                                        "recordId", recordId,
                                        "topic", metadata.topic(),
                                        "partition", metadata.partition(),
                                        "offset", metadata.offset(),
                                        "timestamp", metadata.timestamp(),
                                        "payloadSha256", payloadHash
                                ));
                                confirmed.incrementAndGet();
                            } else {
                                run.ledger().append("OUTCOME_UNKNOWN", Map.of(
                                        "recordId", recordId,
                                        "topic", run.topic(),
                                        "payloadSha256", payloadHash,
                                        "errorType", exception == null ? "MissingRecordMetadata" : exception.getClass().getName(),
                                        "errorMessage", exception == null ? "Producer callback returned no metadata" : safeMessage(exception)
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
                        run.ledger().append("OUTCOME_UNKNOWN", Map.of(
                                "recordId", recordId,
                                "topic", run.topic(),
                                "payloadSha256", payloadHash,
                                "errorType", sendException.getClass().getName(),
                                "errorMessage", safeMessage(sendException)
                        ));
                    } finally {
                        callbacks.countDown();
                    }
                }
            }

            producer.flush();
            boolean completed = callbacks.await(CALLBACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!completed) {
                run.ledger().append("PRODUCER_CALLBACK_TIMEOUT", Map.of(
                        "remainingCallbacks", callbacks.getCount()
                ));
                throw new IllegalStateException("Timed out while waiting for producer callbacks. Remaining outcomes are unresolved.");
            }
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
}
