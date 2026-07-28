package site.yuqi.analytics.aggregator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import site.yuqi.analytics.aggregator.service.ContentIntentMetadataService.ContentIntentMetadata;
import site.yuqi.analytics.aggregator.service.ContentIntentMetadataService.ContentIntentMetadataInput;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ContentIntentMetadataServiceTest {

    @Test
    void writesAndReadsAdminOwnedMetadata() throws Exception {
        MetadataJdbcTemplate jdbc = new MetadataJdbcTemplate();
        ContentIntentMetadataService service =
                new ContentIntentMetadataService(jdbc, new ObjectMapper());
        ContentIntentMetadataInput input = new ContentIntentMetadataInput(
                "PROJECT", "project-1", "/work-single/project-1", "Platform",
                "/cover.png", "career_interest", List.of("Java", "Kafka"),
                "ADVANCED", 90, "projects", 14, 3, true);

        ContentIntentMetadata stored = service.upsert("yuqi.site", input);
        List<ContentIntentMetadata> listed = service.list("yuqi.site", 20);

        assertThat(jdbc.updated).isTrue();
        assertThat(stored.contentId()).isEqualTo("project-1");
        assertThat(stored.technicalDomains()).containsExactly("Java", "Kafka");
        assertThat(stored.metadataSource()).isEqualTo("ADMIN");
        assertThat(listed).containsExactly(stored);
    }

    private static final class MetadataJdbcTemplate extends JdbcTemplate {
        private boolean updated;

        @Override
        public int update(String sql, Object... args) {
            updated = true;
            assertThat(args).contains("yuqi.site", "PROJECT", "project-1", "[\"Java\",\"Kafka\"]");
            return 1;
        }

        @Override
        public <T> List<T> query(String sql, RowMapper<T> mapper, Object... args) {
            try {
                ResultSet resultSet = mock(ResultSet.class);
                Array domains = mock(Array.class);
                when(domains.getArray()).thenReturn(new String[]{"Java", "Kafka"});
                when(resultSet.getString(anyString())).thenAnswer(invocation -> switch (
                        invocation.<String>getArgument(0)) {
                    case "content_type" -> "PROJECT";
                    case "content_id" -> "project-1";
                    case "canonical_path" -> "/work-single/project-1";
                    case "display_title" -> "Platform";
                    case "cover_url" -> "/cover.png";
                    case "primary_intent" -> "career_interest";
                    case "complexity_level" -> "ADVANCED";
                    case "recommendation_group" -> "projects";
                    case "metadata_source" -> "ADMIN";
                    default -> null;
                });
                when(resultSet.getArray("technical_domains")).thenReturn(domains);
                when(resultSet.getInt("career_relevance")).thenReturn(90);
                when(resultSet.getInt("intent_weight")).thenReturn(14);
                when(resultSet.getInt("metadata_version")).thenReturn(3);
                when(resultSet.getBoolean("active")).thenReturn(true);
                when(resultSet.getTimestamp("updated_at"))
                        .thenReturn(Timestamp.from(Instant.parse("2026-07-01T00:00:00Z")));
                return List.of(mapper.mapRow(resultSet, 0));
            } catch (SQLException exception) {
                throw new IllegalStateException(exception);
            }
        }
    }
}
