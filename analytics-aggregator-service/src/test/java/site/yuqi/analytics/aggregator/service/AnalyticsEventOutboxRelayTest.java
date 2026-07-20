package site.yuqi.analytics.aggregator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AnalyticsEventOutboxRelayTest {

    private AnalyticsEventOutboxRepository repository;
    private KafkaTemplate<String, String> kafka;
    private AnalyticsEventOutboxRelay relay;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        repository = mock(AnalyticsEventOutboxRepository.class);
        kafka = mock(KafkaTemplate.class);
        relay = new AnalyticsEventOutboxRelay(repository, kafka);
        ReflectionTestUtils.setField(relay, "rawTopic", "analytics.raw.events");
        ReflectionTestUtils.setField(relay, "batchSize", 10);
        ReflectionTestUtils.setField(relay, "leaseSeconds", 60);
        ReflectionTestUtils.setField(relay, "sendTimeoutSeconds", 1);
    }

    @Test
    void marksSentOnlyAfterBrokerAcknowledgement() {
        var event = event();
        when(repository.claimReady(10, 60)).thenReturn(List.of(event));
        when(kafka.send("analytics.raw.events", "yuqi.site", event.payload()))
                .thenReturn(CompletableFuture.completedFuture(null));

        relay.relay();

        verify(repository).markSent(7L);
        verify(repository, never()).markFailed(org.mockito.ArgumentMatchers.anyLong(), anyString());
    }

    @Test
    void leavesFailedPublishForDurableRetry() {
        var event = event();
        when(repository.claimReady(10, 60)).thenReturn(List.of(event));
        when(kafka.send("analytics.raw.events", "yuqi.site", event.payload()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

        relay.relay();

        verify(repository).markFailed(7L, "broker unavailable");
        verify(repository, never()).markSent(7L);
    }

    private static AnalyticsEventOutboxRepository.OutboxEvent event() {
        return new AnalyticsEventOutboxRepository.OutboxEvent(
                7L, "vl:7", "yuqi.site", "{\"eventId\":\"vl:7\"}", 1);
    }
}
