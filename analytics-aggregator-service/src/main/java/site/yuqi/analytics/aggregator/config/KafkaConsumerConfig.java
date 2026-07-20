package site.yuqi.analytics.aggregator.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/** Container-level seek/retry policy for transient Kafka processing failures. */
@Configuration
@Slf4j
public class KafkaConsumerConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            @Value("${analytics.kafka.retry-interval-ms:2000}") long retryIntervalMs) {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                new FixedBackOff(Math.max(100, retryIntervalMs), FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setRetryListeners((record, error, attempt) -> log.warn(
                "{\"event\":\"kafka_retry\",\"topic\":\"{}\",\"partition\":{},\"offset\":{},\"attempt\":{},\"err\":\"{}\"}",
                record.topic(), record.partition(), record.offset(), attempt, error.getMessage()));
        return handler;
    }
}
