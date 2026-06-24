package site.yuqi.analytics.common.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DlqProducerTest {

    @Test
    @SuppressWarnings("unchecked")
    void publishUsesConfiguredTopicAndKey() {
        KafkaTemplate<String, String> kafka = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);
        when(kafka.send(eq("custom.dlq"), eq("k"), eq("v")))
                .thenReturn(CompletableFuture.completedFuture(null));

        DlqProducer producer = new DlqProducer(kafka, "custom.dlq");

        producer.publish("k", "v", "test-reason");

        verify(kafka).send("custom.dlq", "k", "v");
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishDefaultsToAnalyticsDlqTopic() {
        KafkaTemplate<String, String> kafka = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        DlqProducer producer = new DlqProducer(kafka);

        producer.publish("k", "v", "r");

        verify(kafka).send(Topics.DLQ, "k", "v");
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishSwallowsNullPayloadByCoercingToEmptyString() {
        KafkaTemplate<String, String> kafka = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);
        when(kafka.send(anyString(), any(), eq("")))
                .thenReturn(CompletableFuture.completedFuture(null));

        DlqProducer producer = new DlqProducer(kafka);

        producer.publish("k", null, "r");

        verify(kafka).send(Topics.DLQ, "k", "");
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishNeverThrowsEvenWhenBrokerCallFails() throws Exception {
        KafkaTemplate<String, String> kafka = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new ExecutionException("broker down", new RuntimeException()));
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn((CompletableFuture) failed);

        DlqProducer producer = new DlqProducer(kafka);

        // Contract: never throw, even when the underlying send fails.
        assertThatCode(() -> producer.publish("k", "v", "r")).doesNotThrowAnyException();
    }

    @Test
    @SuppressWarnings("unchecked")
    void dlqTopicAccessorReturnsConfiguredValue() {
        KafkaTemplate<String, String> kafka = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);
        DlqProducer p = new DlqProducer(kafka, "foo");
        org.assertj.core.api.Assertions.assertThat(p.dlqTopic()).isEqualTo("foo");
    }
}
