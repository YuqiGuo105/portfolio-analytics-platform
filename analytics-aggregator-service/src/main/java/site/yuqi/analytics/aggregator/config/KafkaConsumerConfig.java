package site.yuqi.analytics.aggregator.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import site.yuqi.analytics.common.kafka.DlqProducer;

/** Container-level seek/retry policy for transient Kafka processing failures. */
@Configuration
@Slf4j
public class KafkaConsumerConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            @Value("${analytics.kafka.retry-interval-ms:2000}") long retryIntervalMs,
            DlqProducer dlq) {
        ExponentialBackOffWithMaxRetries backoff = new ExponentialBackOffWithMaxRetries(4);
        backoff.setInitialInterval(Math.max(100,retryIntervalMs));
        backoff.setMultiplier(2); backoff.setMaxInterval(15000);
        DefaultErrorHandler handler = new DefaultErrorHandler((record,error) ->
                dlq.publishOrThrow(record.key()==null?null:record.key().toString(),
                        record.value()==null?"":record.value().toString(),"retry_exhausted:"+record.topic()+":"+record.partition()+":"+record.offset()),backoff);
        handler.setCommitRecovered(true);
        handler.setRetryListeners((record, error, attempt) -> log.warn(
                "{\"event\":\"kafka_retry\",\"topic\":\"{}\",\"partition\":{},\"offset\":{},\"attempt\":{},\"err\":\"{}\"}",
                record.topic(), record.partition(), record.offset(), attempt, error.getClass().getSimpleName()));
        return handler;
    }
}
