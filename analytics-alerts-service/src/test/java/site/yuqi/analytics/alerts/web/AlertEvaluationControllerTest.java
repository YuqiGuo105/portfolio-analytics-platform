package site.yuqi.analytics.alerts.web;

import org.junit.jupiter.api.Test;
import site.yuqi.analytics.alerts.service.AlertEvaluator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AlertEvaluationControllerTest {

    @Test
    void evaluatesWithinTheSchedulerRequest() {
        AlertEvaluator evaluator = mock(AlertEvaluator.class);
        AlertEvaluationController controller = new AlertEvaluationController(evaluator);

        assertThat(controller.evaluate()).containsEntry("accepted", true);
        verify(evaluator).tick();
    }
}
