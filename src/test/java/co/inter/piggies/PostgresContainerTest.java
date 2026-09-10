package co.inter.piggies;

import static org.assertj.core.api.Assertions.assertThat;

import co.inter.piggies.support.AbstractContainerTest;
import co.inter.piggies.support.TestContainers;
import io.micronaut.jdbc.DataSourceResolver;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

class PostgresContainerTest extends AbstractContainerTest {

    @Inject
    DataSource dataSource;

    @Inject
    DataSourceResolver dataSourceResolver;

    /**
     * The injected {@code DataSource} is the transaction-aware wrapper, which hands out
     * connections only inside a {@code @Connectable} or {@code @Transactional} scope. Resolving
     * it yields the pooled datasource, so plain JDBC works from a test method.
     */
    private Connection connection() throws SQLException {
        return dataSourceResolver.resolve(dataSource).getConnection();
    }

    @Test
    void runsThePinnedImage() {
        assertThat(TestContainers.POSTGRES.isRunning()).isTrue();
        assertThat(TestContainers.POSTGRES.getDockerImageName()).isEqualTo("postgres:16-alpine");
    }

    @Test
    void datasourcePointsAtTheContainer() throws Exception {
        try (Connection connection = connection();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("select version(), current_database()")) {

            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).startsWith("PostgreSQL 16.");
            assertThat(result.getString(2)).isEqualTo("piggies");
        }
    }

    @Test
    void hibernateCreatedItsSchemaInTheContainer() throws Exception {
        try (Connection connection = connection();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(
                        "select count(*) from information_schema.tables"
                                + " where table_schema = 'public' and table_name = 'piggy'")) {

            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    void acceptsReadsAndWrites() throws Exception {
        try (Connection connection = connection();
                Statement statement = connection.createStatement()) {

            statement.execute("create table if not exists piggy_smoke (id bigint primary key, name varchar(64))");
            statement.execute("insert into piggy_smoke (id, name) values (1, 'Hamm') on conflict (id) do nothing");

            try (ResultSet result = statement.executeQuery("select name from piggy_smoke where id = 1")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString(1)).isEqualTo("Hamm");
            }
        }
    }
}
