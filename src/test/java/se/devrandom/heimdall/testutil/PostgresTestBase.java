package se.devrandom.heimdall.testutil;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Level 3: Shared Testcontainers PostgreSQL setup for integration tests.
 * Extend this class to get a PostgreSQL container available for your tests.
 */
@Testcontainers
public abstract class PostgresTestBase {

    @Container
    protected static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("heimdall_test")
            .withUsername("test")
            .withPassword("test");

    protected String getJdbcUrl() {
        return postgres.getJdbcUrl();
    }

    protected String getUsername() {
        return postgres.getUsername();
    }

    protected String getPassword() {
        return postgres.getPassword();
    }
}
