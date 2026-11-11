package site.yuqi.analytics.aggregator.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import site.yuqi.analytics.aggregator.web.GovernanceAuditInterceptor;

@Configuration
public class GovernanceWebConfig implements WebMvcConfigurer {
    private final GovernanceAuditInterceptor interceptor;
    public GovernanceWebConfig(GovernanceAuditInterceptor interceptor) { this.interceptor = interceptor; }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(interceptor).addPathPatterns("/api/admin/visitors", "/api/admin/visitor-visits",
                "/api/admin/visitor-overview", "/api/admin/visitor-intelligence", "/api/admin/visitor-intelligence/**",
                "/api/admin/visitor-governance", "/api/admin/visitor-governance/**");
    }
}
