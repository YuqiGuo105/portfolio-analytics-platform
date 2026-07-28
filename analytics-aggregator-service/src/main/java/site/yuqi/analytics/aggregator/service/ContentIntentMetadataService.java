package site.yuqi.analytics.aggregator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Admin-owned, model-free content semantics used by runtime intent scoring. */
@Service
@RequiredArgsConstructor
public class ContentIntentMetadataService {

    private static final String SELECT_COLUMNS = """
            content_type, content_id, canonical_path, display_title,
            cover_url, primary_intent, technical_domains,
            complexity_level, career_relevance, recommendation_group,
            intent_weight, metadata_source, metadata_version, active, updated_at
            """;

    private static final String UPSERT_ADMIN = """
            insert into public.content_intent_metadata
              (site_id, content_type, content_id, canonical_path, display_title,
               cover_url, primary_intent, technical_domains, complexity_level,
               career_relevance, recommendation_group, intent_weight,
               metadata_source, metadata_version, active, updated_at)
            values (?, ?, ?, ?, ?, ?, ?,
                    array(select jsonb_array_elements_text(?::jsonb)),
                    ?, ?, ?, ?, 'ADMIN', ?, ?, now())
            on conflict (site_id, content_type, content_id) do update set
              canonical_path = excluded.canonical_path,
              display_title = excluded.display_title,
              cover_url = excluded.cover_url,
              primary_intent = excluded.primary_intent,
              technical_domains = excluded.technical_domains,
              complexity_level = excluded.complexity_level,
              career_relevance = excluded.career_relevance,
              recommendation_group = excluded.recommendation_group,
              intent_weight = excluded.intent_weight,
              metadata_source = 'ADMIN',
              metadata_version = excluded.metadata_version,
              active = excluded.active,
              updated_at = now()
            """;

    private static final String UPSERT_PUBLISHED = """
            insert into public.content_intent_metadata
              (site_id, content_type, content_id, canonical_path, display_title,
               cover_url, primary_intent, technical_domains, complexity_level,
               career_relevance, recommendation_group, intent_weight,
               metadata_source, metadata_version, active, updated_at)
            values (?, ?, ?, ?, ?, ?, ?,
                    array(select jsonb_array_elements_text(?::jsonb)),
                    ?, ?, ?, ?, 'PUBLISH_EVENT', ?, ?, now())
            on conflict (site_id, content_type, content_id) do update set
              canonical_path = excluded.canonical_path,
              display_title = excluded.display_title,
              cover_url = excluded.cover_url,
              primary_intent = excluded.primary_intent,
              technical_domains = excluded.technical_domains,
              complexity_level = excluded.complexity_level,
              career_relevance = excluded.career_relevance,
              recommendation_group = excluded.recommendation_group,
              intent_weight = excluded.intent_weight,
              metadata_source = 'PUBLISH_EVENT',
              metadata_version = excluded.metadata_version,
              active = excluded.active,
              updated_at = now()
            where content_intent_metadata.metadata_source <> 'ADMIN'
              and excluded.metadata_version >= content_intent_metadata.metadata_version
            """;

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public List<ContentIntentMetadata> list(String siteId, int limit) {
        return jdbc.query("""
                        select %s
                          from public.content_intent_metadata
                         where site_id = ?
                         order by updated_at desc, content_type, content_id
                         limit ?
                        """.formatted(SELECT_COLUMNS),
                (rs, rowNum) -> mapRow(rs),
                siteId, limit);
    }

    public Optional<ContentIntentMetadata> find(
            String siteId, String contentType, String contentId) {
        return jdbc.query("""
                        select %s
                          from public.content_intent_metadata
                         where site_id = ?
                           and content_type = ?
                           and content_id = ?
                        """.formatted(SELECT_COLUMNS),
                (rs, rowNum) -> mapRow(rs),
                siteId, contentType, contentId).stream().findFirst();
    }

    @Transactional
    public ContentIntentMetadata upsert(String siteId, ContentIntentMetadataInput input) {
        update(UPSERT_ADMIN, siteId, input);
        return find(siteId, input.contentType(), input.contentId())
                .orElseThrow(() -> new IllegalStateException("Content intent metadata was not persisted"));
    }

    @Transactional
    public ContentIntentMetadata upsertPublished(String siteId, ContentIntentMetadataInput input) {
        update(UPSERT_PUBLISHED, siteId, input);
        return find(siteId, input.contentType(), input.contentId())
                .orElseThrow(() -> new IllegalStateException("Published content metadata was not persisted"));
    }

    private void update(String sql, String siteId, ContentIntentMetadataInput input) {
        jdbc.update(sql,
                siteId,
                input.contentType(),
                input.contentId(),
                input.canonicalPath(),
                input.displayTitle(),
                input.coverUrl(),
                input.primaryIntent(),
                json(input.technicalDomains()),
                input.complexityLevel(),
                input.careerRelevance(),
                input.recommendationGroup(),
                input.intentWeight(),
                input.metadataVersion(),
                input.active());
    }

    private String json(Object value) {
        try {
            return mapper.writeValueAsString(value == null ? List.of() : value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("technicalDomains cannot be serialized", exception);
        }
    }

    private static List<String> array(java.sql.Array value) {
        if (value == null) return List.of();
        try {
            Object raw = value.getArray();
            if (raw instanceof String[] strings) return List.of(strings);
            if (raw instanceof Object[] objects) {
                return java.util.Arrays.stream(objects).map(String::valueOf).toList();
            }
            return List.of();
        } catch (java.sql.SQLException exception) {
            throw new IllegalStateException("technical_domains cannot be read", exception);
        }
    }

    private static ContentIntentMetadata mapRow(ResultSet rs) throws SQLException {
        return new ContentIntentMetadata(
                rs.getString("content_type"),
                rs.getString("content_id"),
                rs.getString("canonical_path"),
                rs.getString("display_title"),
                rs.getString("cover_url"),
                rs.getString("primary_intent"),
                array(rs.getArray("technical_domains")),
                rs.getString("complexity_level"),
                rs.getInt("career_relevance"),
                rs.getString("recommendation_group"),
                rs.getInt("intent_weight"),
                rs.getString("metadata_source"),
                rs.getInt("metadata_version"),
                rs.getBoolean("active"),
                rs.getTimestamp("updated_at").toInstant());
    }

    public record ContentIntentMetadataInput(
            String contentType,
            String contentId,
            String canonicalPath,
            String displayTitle,
            String coverUrl,
            String primaryIntent,
            List<String> technicalDomains,
            String complexityLevel,
            int careerRelevance,
            String recommendationGroup,
            int intentWeight,
            int metadataVersion,
            boolean active) {}

    public record ContentIntentMetadata(
            String contentType,
            String contentId,
            String canonicalPath,
            String displayTitle,
            String coverUrl,
            String primaryIntent,
            List<String> technicalDomains,
            String complexityLevel,
            int careerRelevance,
            String recommendationGroup,
            int intentWeight,
            String metadataSource,
            int metadataVersion,
            boolean active,
            Instant updatedAt) {}
}
