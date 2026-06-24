package site.yuqi.analytics.aggregator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Same Flyway repair-before-migrate strategy as
 * {@code portfolio-notification-service.config.FlywayConfig}. See that
 * class's javadoc for why this exists on Spring Boot 3.3.x.
 */
@Slf4j
@Configuration
public class AggregatorBeansConfig {

    @Bean
    public FlywayMigrationStrategy repairBeforeMigrate() {
        return flyway -> {
            log.info("Flyway: running repair() before migrate() to re-sync schema history checksums.");
            flyway.repair();
            flyway.migrate();
        };
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .findAndRegisterModules();
    }
}
