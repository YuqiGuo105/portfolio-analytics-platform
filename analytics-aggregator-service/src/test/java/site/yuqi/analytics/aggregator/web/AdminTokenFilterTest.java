package site.yuqi.analytics.aggregator.web;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class AdminTokenFilterTest {

    @Test
    void protectsAdminPathAndAcceptsMatchingToken() throws Exception {
        AdminTokenFilter filter = new AdminTokenFilter("server-secret");
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/admin/visitors");
        request.addHeader("X-Internal-Token", "server-secret");
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
    }

    @Test
    void rejectsMissingTokenOnAdminPath() throws Exception {
        AdminTokenFilter filter = new AdminTokenFilter("server-secret");
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/admin/visitors");
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(401);
        verify(chain, never()).doFilter(request, response);
    }

    @Test
    void failsClosedWhenTokenIsNotConfigured() throws Exception {
        AdminTokenFilter filter = new AdminTokenFilter("");
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/admin/visitors");
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(503);
        verify(chain, never()).doFilter(request, response);
    }

    @Test
    void publicPathIsNotFiltered() throws Exception {
        AdminTokenFilter filter = new AdminTokenFilter("");
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/public/visits/summary");
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
    }
}
