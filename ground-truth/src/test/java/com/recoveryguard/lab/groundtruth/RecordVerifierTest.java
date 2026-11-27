package com.recoveryguard.lab.groundtruth;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordVerifierTest {
    @Test
    void includesOnlyRecordsBeforeCapturedEndOffset() {
        Map<TopicPartition, Long> capturedEndOffsets = Map.of(
                new TopicPartition("test-topic", 0), 10L
        );

        ConsumerRecord<String, String> lastIncluded =
                new ConsumerRecord<>("test-topic", 0, 9L, "key-9", "value-9");
        ConsumerRecord<String, String> atEnd =
                new ConsumerRecord<>("test-topic", 0, 10L, "key-10", "value-10");
        ConsumerRecord<String, String> afterEnd =
                new ConsumerRecord<>("test-topic", 0, 11L, "key-11", "value-11");

        assertTrue(RecordVerifier.isBeforeCapturedEndOffset(lastIncluded, capturedEndOffsets));
        assertFalse(RecordVerifier.isBeforeCapturedEndOffset(atEnd, capturedEndOffsets));
        assertFalse(RecordVerifier.isBeforeCapturedEndOffset(afterEnd, capturedEndOffsets));
    }
}
