package ocikit;

import com.google.gson.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

class KafkaConsumerBridgeTest {
    private static JsonObject command(String json) { return JsonParser.parseString(json).getAsJsonObject(); }

    @Test void batchCapAndExplicitCommitsPreserveUnreadRecords() {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        TopicPartition partition = new TopicPartition("events", 0);
        consumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));
        try (KafkaConsumerBridge bridge = new KafkaConsumerBridge(consumer)) {
            bridge.handle(command("{op:'subscribe',topics:['events']}"));
            consumer.schedulePollTask(() -> {
                consumer.rebalance(Collections.singleton(partition));
                for (int offset = 0; offset < 3; offset++) consumer.addRecord(new ConsumerRecord<>("events", 0, offset, null, "hello world".getBytes(StandardCharsets.UTF_8)));
            });
            JsonArray first = bridge.handle(command("{op:'consume',limit:1,timeout_ms:10}")).getAsJsonArray("records");
            assertEquals(1, first.size());
            assertEquals(0, first.get(0).getAsJsonObject().get("offset").getAsInt());
            assertTrue(first.get(0).getAsJsonObject().get("key").isJsonNull());
            assertEquals("aGVsbG8gd29ybGQ=", first.get(0).getAsJsonObject().get("value").getAsString());
            assertNull(consumer.committed(Collections.singleton(partition)).get(partition));
            bridge.handle(command("{op:'commit',offsets:[{topic:'events',partition:0,offset:1}]}"));
            assertEquals(1, consumer.committed(Collections.singleton(partition)).get(partition).offset());
            JsonArray rest = bridge.handle(command("{op:'consume',limit:2,timeout_ms:10}")).getAsJsonArray("records");
            assertEquals(2, rest.size());
            assertEquals(1, rest.get(0).getAsJsonObject().get("offset").getAsInt());
        }
        assertTrue(consumer.closed());
    }

    @Test void secureConfigurationDisablesAutomaticCommitsAndQuotesCredentials() {
        JsonObject config = command("{bootstrap:'host:9092',group:'group',instance:'worker-0',username:'tenant/user/pool',cursor_type:'LATEST',batch_size:1000,connect_timeout_ms:5000,request_timeout_ms:30000}");
        Properties props = KafkaConsumerBridge.properties(config, "token\\\"quoted");
        assertEquals("false", props.get("enable.auto.commit"));
        assertEquals("SASL_SSL", props.get("security.protocol"));
        assertEquals("PLAIN", props.get("sasl.mechanism"));
        assertEquals("latest", props.get("auto.offset.reset"));
        assertTrue(props.getProperty("sasl.jaas.config").contains("password=\"token\\\\\\\"quoted\""));
        assertDoesNotThrow(() -> new ConsumerConfig(props));
    }

    @Test void emptyReadsDoNotCommitAndInvalidOperationsFail() {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        try (KafkaConsumerBridge bridge = new KafkaConsumerBridge(consumer)) {
            bridge.handle(command("{op:'subscribe',topics:['events']}"));
            assertEquals(0, bridge.handle(command("{op:'consume',limit:1,timeout_ms:1}")).getAsJsonArray("records").size());
            assertThrows(IllegalArgumentException.class, () -> bridge.handle(command("{op:'consume',limit:0,timeout_ms:1}")));
            assertThrows(IllegalArgumentException.class, () -> bridge.handle(command("{op:'bad'}")));
        }
    }

    @Test void timestampStartPreservesExistingCommits() {
        TopicPartition old = new TopicPartition("events", 0);
        TopicPartition fresh = new TopicPartition("events", 1);
        final Map<TopicPartition, Long> lookedUp = new HashMap<>();
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            @Override public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestamps, Duration timeout) {
                lookedUp.putAll(timestamps);
                return Collections.singletonMap(fresh, new OffsetAndTimestamp(20, timestamps.get(fresh)));
            }
        };
        Map<TopicPartition, Long> starts = new HashMap<>();
        starts.put(old, 0L); starts.put(fresh, 0L);
        consumer.updateBeginningOffsets(starts);
        try (KafkaConsumerBridge bridge = new KafkaConsumerBridge(consumer, 123456L)) {
            bridge.handle(command("{op:'subscribe',topics:['events']}"));
            consumer.commitSync(Collections.singletonMap(old, new OffsetAndMetadata(7)));
            consumer.schedulePollTask(() -> consumer.rebalance(Arrays.asList(old, fresh)));
            bridge.handle(command("{op:'consume',limit:1,timeout_ms:1}"));
            assertEquals(Collections.singletonMap(fresh, 123456L), lookedUp);
            assertEquals(7, consumer.position(old));
            assertEquals(20, consumer.position(fresh));
        }
    }
}
