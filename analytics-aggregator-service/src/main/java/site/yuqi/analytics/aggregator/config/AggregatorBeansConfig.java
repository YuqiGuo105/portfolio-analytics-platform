package site.yuqi.analytics.aggregator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import site.yuqi.analytics.aggregator.enrich.IpHashService;
import site.yuqi.analytics.common.kafka.DlqProducer;

/**
 * Beans shared across the aggregator service. The Flyway strategy mirrors
 * {@code portfolio-notification-service.config.FlywayConfig} — repair the
 * schema-history checksums before migrate() so a manual hot-fix migration
 * file that drifted from the source-of-truth doesn't break boot.
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

    @Bean
    public IpHashService ipHashService(@Value("${analytics.hmac-salt}") String salt) {
        return new IpHashService(salt);
    }

    @Bean
    public DlqProducer dlqProducer(KafkaTemplate<String, String> kafka,
                                   @Value("${analytics.topics.dlq}") String dlqTopic) {
        return new DlqProducer(kafka, dlqTopic);
    }
}
