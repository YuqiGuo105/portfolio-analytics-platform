package site.yuqi.analytics.aggregator.web;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class AdminTokenFilterTest {

    @ParameterizedTest
    @ValueSource(strings = {"/api/%61dmin/visitors", "/api/%2561dmin/visitors", "/api/admin;v=1/visitors",
            "/api/public/../admin/visitors", "//api/admin/visitors", "/api/%69nternal/workers/drain"})
    void ambiguousPathsNeverReachControllers(String path) throws Exception {
        var filter = new AdminTokenFilter("server-secret");
        var request = new MockHttpServletRequest("GET", path);
        request.addHeader("X-Internal-Token", "server-secret");
        var response = new MockHttpServletResponse();
        var chain = mock(FilterChain.class);
        filter.doFilter(request, response, chain);
        assertThat(response.getStatus()).isEqualTo(400);
        verify(chain, never()).doFilter(request, response);
    }

    @Test
    void unknownPathsAreNotPublicByDefault() throws Exception {
        var request = new MockHttpServletRequest("GET", "/new-private-api");
        var response = new MockHttpServletResponse();
        var chain = mock(FilterChain.class);
        new AdminTokenFilter("server-secret").doFilter(request, response, chain);
        assertThat(response.getStatus()).isEqualTo(401);
        verify(chain, never()).doFilter(request, response);
    }

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
