package site.yuqi.analytics.alerts.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.*;
import site.yuqi.analytics.alerts.dto.*;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class SourceFilterTest {
    EmbeddedDatabase db;
    JdbcTemplate jdbc;
    AlertRuleRepository repo;
    AlertRuleChangeService service;
    final Instant bucket = Instant.parse("2026-10-11T12:00:00Z");

    @BeforeEach void setup() {
        db = new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2)
                .setName("source-filter;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
                .addScript("classpath:test-schema.sql").build();
        jdbc = new JdbcTemplate(db); repo = new AlertRuleRepository(jdbc, db);
        service = new AlertRuleChangeService(repo, jdbc, new ObjectMapper());
        jdbc.execute("create table behavior_events(event_id text,site_id text,event_name text,referrer_domain text,is_bot boolean,country text,region text,geo_area_id text,event_time timestamp with time zone)");
    }
    @AfterEach void close() { db.shutdown(); }

    AlertRule rule(AlertRuleFilters.Bot bot, String source) {
        return repo.insert(new AlertRuleRequest("test", "Jaipur", "page_view", "REGION", "REGION:IN:RJ", "5m", 1, ">=", 1800, new AlertRuleFilters(bot, source)));
    }
    void event(String id, String source, Boolean bot, String country, String region, Instant time) {
        jdbc.update("insert into behavior_events values (?,?,?,?,?,?,?,?,?)", id, "test", "page_view", source, bot, country, region, "METRO:IN:RJ:Jaipur", Timestamp.from(time));
    }
    @Test void exactSourceBotGeographyAndExclusiveWindowAreAppliedTogetherInOneBatch() {
        AlertRule people = rule(AlertRuleFilters.Bot.EXCLUDE, "linkedin.com");
        AlertRule bots = rule(AlertRuleFilters.Bot.ONLY, "linkedin.com");
        AlertRule all = rule(AlertRuleFilters.Bot.ALL, "linkedin.com");
        event("person", "LinkedIn.COM", false, "IN", "RJ", bucket);
        event("bot", "linkedin.com", true, "IN", "RJ", bucket.plusSeconds(1));
        event("unknown", "linkedin.com", null, "IN", "RJ", bucket.plusSeconds(2));
        event("other-source", "linkedin.com.evil.test", false, "IN", "RJ", bucket);
        event("other-country", "linkedin.com", false, "US", "RJ", bucket);
        event("boundary", "linkedin.com", false, "IN", "RJ", bucket.plusSeconds(300));
        event("previous", "linkedin.com", false, "IN", "RJ", bucket.minusSeconds(1));
        var counts = SourceFilteredMeasurements.count(jdbc, List.of(people, bots, all), bucket);
        assertThat(counts).containsEntry(people.ruleId(), 1L).containsEntry(bots.ruleId(), 1L).containsEntry(all.ruleId(), 3L);
    }
    @Test void preparedBotOnlyUpdatePreservesSourceAndExplicitClearSurvivesDurableSerialization() throws Exception {
        AlertRule original = rule(AlertRuleFilters.Bot.EXCLUDE, "LinkedIn.COM");
        var mapper = new ObjectMapper();
        var prepared = service.prepare(new PrepareChangeRequest("UPDATE", original.ruleId(), mapper.readValue("{\"filters\":{\"bot\":\"ONLY\"}}", AlertRulePatch.class), "bot change", "admin"));
        assertThat(prepared.after().get("filters")).isEqualTo(java.util.Map.of("bot", "ONLY", "referrerDomain", "linkedin.com"));
        service.apply(new ApplyChangeRequest(prepared.changeId(), "source-preserve"));
        assertThat(repo.findById(original.ruleId()).orElseThrow().filters().referrerDomain()).isEqualTo("linkedin.com");
        var cleared = service.prepare(new PrepareChangeRequest("UPDATE", original.ruleId(), mapper.readValue("{\"filters\":{\"bot\":\"ONLY\",\"referrerDomain\":\"\"}}", AlertRulePatch.class), "clear source", "admin"));
        service.apply(new ApplyChangeRequest(cleared.changeId(), "source-clear"));
        assertThat(repo.findById(original.ruleId()).orElseThrow().filters().referrerDomain()).isEmpty();
    }
    @Test void invalidHostsFailClosed() {
        for (String source : List.of("https://linkedin.com", "linkedin.com/path", "a@linkedin.com", "bad host", "-bad.com"))
            assertThatThrownBy(() -> new AlertRuleFilters(AlertRuleFilters.Bot.ALL, source)).isInstanceOf(IllegalArgumentException.class);
    }
}
