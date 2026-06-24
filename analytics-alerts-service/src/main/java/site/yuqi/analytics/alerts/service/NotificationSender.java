package site.yuqi.analytics.alerts.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.Map;

/**
 * Wraps an outbound POST to portfolio-notification-service's
 * {@code /api/content-events} endpoint. Best-effort — if the call fails,
 * the incident row in Postgres still records {@code notified = false}, so
 * the next eval tick will pick it up and try again. We never throw.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationSender {

    private static final String PATH = "/api/content-events";

    private final RestClient notificationRestClient;

    @Value("${analytics.notification.internal-token}")
    private String token;

    /** @return true on 2xx, false otherwise (caller decides whether to mark notified). */
    public boolean send(Map<String, Object> payload) {
        try {
            notificationRestClient
                    .post()
                    .uri(PATH)
                    .header("X-Internal-Token", token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(payload)
                    .retrieve()
                    .toBodilessEntity();
            return true;
        } catch (RuntimeException e) {
            log.warn("{\"event\":\"notification_send_failed\",\"err\":\"{}\"}", e.getMessage());
            return false;
        }
    }
}
