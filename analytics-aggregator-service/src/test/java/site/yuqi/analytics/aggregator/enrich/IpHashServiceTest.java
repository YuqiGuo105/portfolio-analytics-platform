package site.yuqi.analytics.aggregator.enrich;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IpHashServiceTest {

    @Test
    void blankInputProducesNull() {
        IpHashService svc = new IpHashService("salt");
        assertThat(svc.hash(null)).isNull();
        assertThat(svc.hash("")).isNull();
        assertThat(svc.hash("   ")).isNull();
    }

    @Test
    void sameInputProducesSameHash() {
        IpHashService svc = new IpHashService("salt-A");
        assertThat(svc.hash("1.2.3.4")).isEqualTo(svc.hash("1.2.3.4"));
    }

    @Test
    void differentSaltProducesDifferentHashForSameIp() {
        assertThat(new IpHashService("A").hash("1.2.3.4"))
                .isNotEqualTo(new IpHashService("B").hash("1.2.3.4"));
    }

    @Test
    void outputIsHexAndExpectedLength() {
        String h = new IpHashService("salt").hash("1.2.3.4");
        // HMAC-SHA-256 => 32 bytes => 64 hex chars
        assertThat(h).hasSize(64).matches("[0-9a-f]+");
    }
}
