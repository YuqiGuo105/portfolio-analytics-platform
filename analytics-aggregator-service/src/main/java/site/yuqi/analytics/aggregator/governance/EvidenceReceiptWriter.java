package site.yuqi.analytics.aggregator.governance;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import site.yuqi.analytics.common.event.EnrichedEvent;

import java.util.List;

@Service
public class EvidenceReceiptWriter {
    private final JdbcTemplate jdbc;
    private final GovernancePolicy policy;

    public EvidenceReceiptWriter(JdbcTemplate jdbc, GovernancePolicy policy) {
        this.jdbc = jdbc;
        this.policy = policy;
    }

    /** Exactly one extra SQL append for the existing batch, with no preliminary DB read. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void append(List<EnrichedEvent> events) {
        if (!policy.enabled() || events.isEmpty()) return;
        String ids = EvidenceCodec.canonical(events.stream().map(EnrichedEvent::eventId).distinct().toList());
        jdbc.update("""
                insert into analytics_private.visitor_evidence_receipts
                    (site_id, source_event_id, event_ref, expires_at, canonical_evidence, evidence_sha256)
                select e.site_id, e.event_id, encode(sha256(convert_to(e.event_id, 'UTF8')), 'hex'),
                    statement_timestamp() + make_interval(days => ?), p.payload,
                    encode(sha256(convert_to(p.payload, 'UTF8')), 'hex')
                from public.behavior_events e
                cross join lateral (select analytics_private.visitor_evidence_v1(e, ?) as payload) p
                where e.event_id in (select jsonb_array_elements_text(?::jsonb)) and e.site_id = ?
                on conflict (event_ref) do nothing
                """, policy.receiptRetentionDays(), policy.siteId(), ids, policy.siteId());
    }
}
