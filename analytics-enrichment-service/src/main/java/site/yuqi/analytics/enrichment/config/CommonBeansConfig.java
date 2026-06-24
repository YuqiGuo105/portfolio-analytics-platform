package site.yuqi.analytics.enrichment.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import site.yuqi.analytics.common.kafka.DlqProducer;

@Configuration
public class CommonBeansConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .findAndRegisterModules();
    }

    @Bean
    public DlqProducer dlqProducer(
            KafkaTemplate<String, String> kafka,
            @org.springframework.beans.factory.annotation.Value("${analytics.topics.dlq}") String dlqTopic) {
        return new DlqProducer(kafka, dlqTopic);
    }
}
