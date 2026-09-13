package site.yuqi.analytics.aggregator.enrich;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class UaParserServiceTest {

    private final UaParserService parser = new UaParserService();

    @ParameterizedTest
    @ValueSource(strings = {"meta-externalagent/1.1", "META-EXTERNALAGENT/1.1", "meta-externalfetcher/1.1",
            "meta-webindexer/1.0", "facebookexternalhit/1.1", "Googlebot/2.1", "GPTBot/1.0",
            "ClaudeBot/1.0", "bingbot/2.0", "HeadlessChrome/145.0"})
    void detectsAutomationProductInsideBrowserLikeUa(String product) {
        String browser = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                + "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 (compatible; " + product + ")";
        assertThat(parser.parse(product).deviceType()).isEqualTo("bot");
        var parsed = parser.parse(browser);
        assertThat(parsed.deviceType()).isEqualTo("bot");
        assertThat(new BotScoreService().isBot(new BotScoreService().score(parsed.deviceType(), ""))).isTrue();
    }

    @Test void unrelatedProductWordsAreNotMistakenForCrawlerTokens() {
        assertThat(parser.parse("Mozilla/5.0 RoboticsResearch/1.0 Chrome/145.0").deviceType()).isEqualTo("desktop");
    }

    @Test
    void blankUaIsAllUnknown() {
        UaParserService.Parsed p = parser.parse("");
        assertThat(p.deviceType()).isEqualTo("unknown");
        assertThat(p.browser()).isEqualTo("unknown");
        assertThat(p.os()).isEqualTo("unknown");
    }

    @Test
    void recognisesChromeOnMac() {
        UaParserService.Parsed p = parser.parse(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                        + "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36");
        assertThat(p.deviceType()).isEqualTo("desktop");
        assertThat(p.browser()).isEqualTo("Chrome");
        assertThat(p.os()).isEqualTo("macOS");
    }

    @Test
    void recognisesMobileSafariOnIphone() {
        UaParserService.Parsed p = parser.parse(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
                        + "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1");
        assertThat(p.deviceType()).isEqualTo("mobile");
        assertThat(p.browser()).isEqualTo("Safari");
        assertThat(p.os()).isEqualTo("iOS");
    }

    @Test
    void recognisesBotPattern() {
        UaParserService.Parsed p = parser.parse("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)");
        assertThat(p.deviceType()).isEqualTo("bot");
    }

    @Test
    void recognisesIpadAsTablet() {
        UaParserService.Parsed p = parser.parse("Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) Safari/604.1");
        assertThat(p.deviceType()).isEqualTo("tablet");
    }

    @Test
    void recognisesEdgeFirefoxOperaWindowsLinuxAndroid() {
        assertThat(parser.parse("Mozilla/5.0 (Windows NT 10.0) Edg/126.0").browser()).isEqualTo("Edge");
        assertThat(parser.parse("Mozilla/5.0 (Windows NT 10.0) Firefox/126.0").browser()).isEqualTo("Firefox");
        assertThat(parser.parse("Mozilla/5.0 OPR/110.0").browser()).isEqualTo("Opera");
        assertThat(parser.parse("Mozilla/5.0 (Windows NT 10.0; Win64; x64)").os()).isEqualTo("Windows");
        assertThat(parser.parse("Mozilla/5.0 (X11; Linux x86_64)").os()).isEqualTo("Linux");
        assertThat(parser.parse("Mozilla/5.0 (Linux; Android 14; Pixel 8) Chrome/126.0 Mobile").os()).isEqualTo("Android");
    }
}
