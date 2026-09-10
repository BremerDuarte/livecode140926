package co.inter.piggies.support;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Singleton containers shared by every test in the JVM.
 *
 * <p>They start in parallel the first time this class is loaded and are removed by the
 * Testcontainers reaper once the JVM exits, so a full test run pays the startup cost once
 * instead of once per test class.
 *
 * <p>Both tags are pinned to images that are already pulled locally.
 */
public final class TestContainers {

    private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse("postgres:16-alpine");

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("apache/kafka-native:4.3.1");

    public static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(POSTGRES_IMAGE)
            .withDatabaseName("piggies")
            .withUsername("piggies")
            .withPassword("piggies");

    public static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_IMAGE);

    static {
        Startables.deepStart(POSTGRES, KAFKA).join();
    }

    private TestContainers() {
    }

    /**
     * Creates a topic up front so listeners never race the broker's topic auto-creation.
     *
     * <p>Existing topics are left untouched, which keeps this safe to call from more than one
     * test class against the shared broker.
     */
    public static void createTopic(String name, int partitions) {
        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

        try (Admin admin = Admin.create(config)) {
            admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating topic " + name, e);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("Could not create topic " + name, e);
            }
        }
    }
}
