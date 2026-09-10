package co.inter.piggies;

import static org.assertj.core.api.Assertions.assertThat;

import co.inter.piggies.support.AbstractContainerTest;
import co.inter.piggies.support.TestContainers;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class KafkaContainerTest extends AbstractContainerTest {

    static final String TOPIC = "piggies";

    private static final String CONSUMER_ENABLED = "piggies.test.consumer.enabled";

    @Inject
    PiggyProducer producer;

    @Inject
    PiggyConsumer consumer;

    /**
     * Micronaut calls this before the context starts, which is the only window in which the
     * topic can be created ahead of the listener subscribing to it.
     */
    @Override
    public Map<String, String> getProperties() {
        TestContainers.createTopic(TOPIC, 1);

        Map<String, String> properties = new HashMap<>(super.getProperties());
        properties.put(CONSUMER_ENABLED, "true");
        return properties;
    }

    @Test
    void runsThePinnedImage() {
        assertThat(TestContainers.KAFKA.isRunning()).isTrue();
        assertThat(TestContainers.KAFKA.getDockerImageName()).isEqualTo("apache/kafka-native:4.3.1");
    }

    @Test
    void roundTripsAMessageThroughTheBroker() {
        producer.send("piggy-1", "Hamm");
        producer.send("piggy-2", "Evelyn");

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(consumer.received()).contains("Hamm", "Evelyn"));
    }

    @KafkaClient
    interface PiggyProducer {

        @Topic(TOPIC)
        void send(@KafkaKey String key, String name);
    }

    /**
     * Only active for this test, so the other container tests do not start a consumer group.
     */
    @Requires(property = CONSUMER_ENABLED, value = "true")
    @KafkaListener(groupId = "piggies-container-test", offsetReset = OffsetReset.EARLIEST)
    static class PiggyConsumer {

        private final Queue<String> received = new ConcurrentLinkedQueue<>();

        @Topic(TOPIC)
        void receive(String name) {
            received.add(name);
        }

        Queue<String> received() {
            return received;
        }
    }
}
