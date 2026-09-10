package co.inter.piggies.support;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import java.util.Map;
import org.junit.jupiter.api.TestInstance;

/**
 * Base class for tests that need the real PostgreSQL and Kafka brokers.
 *
 * <p>Extending it wires the running containers into the application context, replacing the
 * {@code localhost} coordinates from {@code application.yml} with the randomly mapped ports
 * Docker handed out.
 */
@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractContainerTest implements TestPropertyProvider {

    @Override
    public Map<String, String> getProperties() {
        return Map.of(
                "datasources.default.url", TestContainers.POSTGRES.getJdbcUrl(),
                "datasources.default.username", TestContainers.POSTGRES.getUsername(),
                "datasources.default.password", TestContainers.POSTGRES.getPassword(),
                "datasources.default.driver-class-name", TestContainers.POSTGRES.getDriverClassName(),
                "kafka.bootstrap.servers", TestContainers.KAFKA.getBootstrapServers());
    }
}
