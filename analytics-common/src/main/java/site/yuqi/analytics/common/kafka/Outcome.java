package site.yuqi.analytics.common.kafka;

/**
 * The three-state outcome contract every {@code @KafkaListener} in this
 * platform uses, modelled exactly after
 * {@code portfolio-notification-service}'s {@code ContentEventProcessor.Outcome}.
 *
 * <ul>
 *   <li>{@link #DONE}  — message handled (or intentionally ignored as a dup). Ack now.</li>
 *   <li>{@link #DLQ}   — message is a poison pill. Publish to DLQ then ack.</li>
 *   <li>{@link #RETRY} — transient failure. Do NOT ack; let the container redeliver.</li>
 * </ul>
 */
public enum Outcome {
    DONE,
    DLQ,
    RETRY
}
