package site.yuqi.analytics.enrichment.service;

/**
 * Heuristic bot likelihood scorer. The score is recorded on every
 * enriched event and the {@code bot} boolean flips on at {@code >= 0.8}
 * so the aggregator can split the {@code bot_ratio} metric.
 *
 * <p>Inputs are deliberately small: UA classification (from
 * {@link UaParserService}) and whether the request has no Referer. The
 * production wiring extends this with ASN scoring + Cloudflare bot
 * scores, but the contract for the rest of the pipeline doesn't change.
 */
public class BotScoreService {

    public double score(String deviceType, String referrer) {
        double s = 0.0;
        if ("bot".equals(deviceType)) {
            s = 1.0;
        } else if ("unknown".equals(deviceType)) {
            s = 0.5;
        }
        // No referrer + non-mobile is a weak bot signal — bump only a little.
        if ((referrer == null || referrer.isBlank()) && !"mobile".equals(deviceType)) {
            s = Math.min(1.0, s + 0.1);
        }
        return s;
    }

    public boolean isBot(double score) {
        return score >= 0.8;
    }
}
