package site.yuqi.analytics.enrichment.service;

import java.util.regex.Pattern;

/**
 * Minimal User-Agent classifier. This is intentionally a small heuristic —
 * the production wiring (TODO) is to drop {@code com.github.ua-parser:uap-java}
 * in here and back this class with a YAML regex pack. Keeping the surface
 * small means the contract with {@link EnrichmentProcessor} is the only
 * thing tests pin down.
 */
public class UaParserService {

    private static final Pattern BOT_PATTERN = Pattern.compile(
            "(?i)\\b(bot|spider|crawl|scraper|preview|monitor|http|wget|curl|python|go-http|java/)\\b");
    private static final Pattern MOBILE_PATTERN = Pattern.compile(
            "(?i)\\b(iphone|android|mobile|ipod)\\b");
    private static final Pattern TABLET_PATTERN = Pattern.compile(
            "(?i)\\b(ipad|tablet)\\b");

    public record Parsed(String deviceType, String browser, String os) {}

    public Parsed parse(String ua) {
        if (ua == null || ua.isBlank()) {
            return new Parsed("unknown", "unknown", "unknown");
        }
        String deviceType = classifyDevice(ua);
        return new Parsed(deviceType, classifyBrowser(ua), classifyOs(ua));
    }

    private String classifyDevice(String ua) {
        if (BOT_PATTERN.matcher(ua).find()) return "bot";
        if (TABLET_PATTERN.matcher(ua).find()) return "tablet";
        if (MOBILE_PATTERN.matcher(ua).find()) return "mobile";
        return "desktop";
    }

    private String classifyBrowser(String ua) {
        if (containsCi(ua, "Edg/")) return "Edge";
        if (containsCi(ua, "OPR/") || containsCi(ua, "Opera")) return "Opera";
        if (containsCi(ua, "Firefox/")) return "Firefox";
        if (containsCi(ua, "Chrome/")) return "Chrome";
        if (containsCi(ua, "Safari/")) return "Safari";
        return "unknown";
    }

    private String classifyOs(String ua) {
        if (containsCi(ua, "Windows")) return "Windows";
        // iOS check must come BEFORE macOS — iPhone/iPad UAs contain the literal
        // string "Mac OS X" (e.g. "CPU iPhone OS 17_0 like Mac OS X").
        if (containsCi(ua, "iPhone") || containsCi(ua, "iPad") || containsCi(ua, "iPod")) return "iOS";
        if (containsCi(ua, "Mac OS X") || containsCi(ua, "Macintosh")) return "macOS";
        // Android check must come BEFORE Linux — Android UAs say "Linux; Android".
        if (containsCi(ua, "Android")) return "Android";
        if (containsCi(ua, "Linux")) return "Linux";
        return "unknown";
    }

    private static boolean containsCi(String haystack, String needle) {
        return haystack.toLowerCase().contains(needle.toLowerCase());
    }
}
