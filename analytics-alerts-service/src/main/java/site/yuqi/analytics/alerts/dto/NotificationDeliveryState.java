package site.yuqi.analytics.alerts.dto;

public enum NotificationDeliveryState {
    PENDING,
    DELIVERING,
    RETRY_WAIT,
    DELIVERED,
    DEAD_LETTER
}
