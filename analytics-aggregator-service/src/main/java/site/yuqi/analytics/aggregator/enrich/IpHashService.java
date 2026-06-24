package site.yuqi.analytics.aggregator.enrich;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * One-way hashes a raw IP into a stable opaque token. The plaintext IP is
 * dropped on the floor by the caller immediately after.
 *
 * <p>HMAC-SHA-256 with a server-side salt — not plain SHA-256 — because the
 * IPv4 space is small enough (~4B addresses) that an unsalted hash is
 * trivially rainbow-tableable. Rotating {@code analytics.hmac-salt}
 * invalidates historical {@code ip_hash} joins, which is the intended
 * privacy escape hatch.
 */
public class IpHashService {

    private static final HexFormat HEX = HexFormat.of();

    private final byte[] saltBytes;

    public IpHashService(String salt) {
        this.saltBytes = (salt == null ? "" : salt).getBytes(StandardCharsets.UTF_8);
    }

    /** @return lowercase hex of HMAC-SHA-256(salt, ip), or {@code null} when {@code ip} is null/blank. */
    public String hash(String ip) {
        if (ip == null || ip.isBlank()) return null;
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(saltBytes, "HmacSHA256"));
            byte[] out = mac.doFinal(ip.trim().getBytes(StandardCharsets.UTF_8));
            return HEX.formatHex(out);
        } catch (NoSuchAlgorithmException | java.security.InvalidKeyException e) {
            // HmacSHA256 is mandatory in every JDK; this branch is unreachable.
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }
}
