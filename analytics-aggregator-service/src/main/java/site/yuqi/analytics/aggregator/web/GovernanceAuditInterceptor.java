package site.yuqi.analytics.aggregator.web;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import site.yuqi.analytics.aggregator.governance.GovernanceAudit;
import site.yuqi.analytics.aggregator.governance.GovernanceException;
import site.yuqi.analytics.aggregator.governance.GovernancePolicy;

import java.util.UUID;

import static site.yuqi.analytics.aggregator.governance.GovernanceAudit.*;

/** Runs inside MVC, after the existing server-token servlet filter. */
@Component
public class GovernanceAuditInterceptor implements HandlerInterceptor {
    private final GovernancePolicy policy;
    private final GovernanceAudit audit;

    public GovernanceAuditInterceptor(GovernancePolicy policy, GovernanceAudit audit) {
        this.policy = policy;
        this.audit = audit;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String path = request.getRequestURI().substring(request.getContextPath().length());
        boolean governance = path.equals("/api/admin/visitor-governance")
                || path.startsWith("/api/admin/visitor-governance/");
        if (governance) {
            response.setHeader("Cache-Control", "no-store");
            if (!policy.enabled()) throw new GovernanceException(404, "governance_disabled", "Visitor governance is disabled");
            GovernanceAudit.actor(request.getHeader("X-Admin-Actor"));
            return true;
        }
        if (!policy.enabled() || !(request.getMethod().equals("GET") || request.getMethod().equals("HEAD"))) return true;
        Target target = switch (path) {
            case "/api/admin/visitors" -> Target.VISITORS;
            case "/api/admin/visitor-visits" -> Target.VISITOR_VISITS;
            case "/api/admin/visitor-overview" -> Target.VISITOR_OVERVIEW;
            default -> path.equals("/api/admin/visitor-intelligence") || path.startsWith("/api/admin/visitor-intelligence/")
                    ? Target.VISITOR_INTELLIGENCE : null;
        };
        if (target == null) return true;
        response.setHeader("Cache-Control", "no-store");
        String actor = GovernanceAudit.actor(request.getHeader("X-Admin-Actor"));
        String suppliedAction = request.getHeader("X-Admin-Action");
        Action action = switch (suppliedAction == null ? "visitor_read" : suppliedAction) {
            case "visitor_read" -> Action.VISITOR_READ;
            case "visitor_export" -> Action.VISITOR_EXPORT;
            default -> throw GovernanceException.invalid();
        };
        String requestId = request.getHeader("X-Admin-Request-Id");
        UUID correlation = requestId == null ? null : AdminVisitorGovernanceController.uuid(requestId);
        String format = request.getHeader("X-Admin-Export-Format");
        if (format != null && !format.equals("csv") && !format.equals("json")) throw GovernanceException.invalid();
        try {
            audit.append(actor, action, target, null, null, 0, Outcome.REQUESTED, correlation,
                    action == Action.VISITOR_EXPORT ? format : null);
        } catch (DataAccessException unavailable) {
            throw new GovernanceException(503, "governance_unavailable", "Visitor access audit is unavailable");
        }
        return true;
    }
}
