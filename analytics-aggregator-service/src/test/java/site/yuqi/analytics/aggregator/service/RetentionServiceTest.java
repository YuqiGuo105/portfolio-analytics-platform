package site.yuqi.analytics.aggregator.service;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class RetentionServiceTest {

    @Test
    void disabledDoesNothing() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        RetentionService svc = new RetentionService(jdbc, false, 3, 2000, 50000);
        svc.purgeStale5mRows();
        verify(jdbc, never()).update(anyString(), anyInt(), anyInt());
    }

    @Test
    void deletesInBatchesUntilExhausted() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        // First batch: 500 deleted, second batch: 200 (< batchSize) → stop.
        when(jdbc.update(anyString(), anyInt(), anyInt()))
                .thenReturn(500)
                .thenReturn(200);

        RetentionService svc = new RetentionService(jdbc, true, 3, 500, 50000);
        svc.purgeStale5mRows();

        verify(jdbc, times(2)).update(anyString(), anyInt(), anyInt());
    }

    @Test
    void respectsMaxPerRun() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        // Every batch deletes the full batch size → must stop at maxPerRun
        when(jdbc.update(anyString(), anyInt(), anyInt())).thenReturn(1000);

        RetentionService svc = new RetentionService(jdbc, true, 3, 1000, 3000);
        svc.purgeStale5mRows();

        // maxPerRun=3000 / batchSize=1000 = 3 iterations
        verify(jdbc, times(3)).update(anyString(), anyInt(), anyInt());
    }
}
