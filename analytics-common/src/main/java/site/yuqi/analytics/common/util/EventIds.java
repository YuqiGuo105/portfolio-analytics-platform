package site.yuqi.analytics.common.util;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;

/**
 * UUIDv7 generator — time-ordered (millisecond-precision) random UUIDs.
 *
 * <p>v7 is the right choice here because every analytics record is keyed by
 * {@code event_id} in Postgres and time-ordered values keep the B-tree
 * insert sequential. Java's standard library only ships v4, so we generate
 * the layout by hand following RFC 9562 §5.7.
 */
public final class EventIds {

    private static final SecureRandom RANDOM = new SecureRandom();

    private EventIds() {
        // utility
    }

    /** Generate a fresh UUIDv7 string. */
    public static String newId() {
        return newId(Instant.now()).toString();
    }

    /** Visible for testing. */
    static UUID newId(Instant when) {
        long unixMillis = when.toEpochMilli();

        byte[] bytes = new byte[16];
        // 48-bit timestamp in big-endian
        bytes[0] = (byte) ((unixMillis >> 40) & 0xFF);
        bytes[1] = (byte) ((unixMillis >> 32) & 0xFF);
        bytes[2] = (byte) ((unixMillis >> 24) & 0xFF);
        bytes[3] = (byte) ((unixMillis >> 16) & 0xFF);
        bytes[4] = (byte) ((unixMillis >> 8) & 0xFF);
        bytes[5] = (byte) (unixMillis & 0xFF);

        byte[] rand = new byte[10];
        RANDOM.nextBytes(rand);
        System.arraycopy(rand, 0, bytes, 6, 10);

        // Set version (7) in the top 4 bits of byte 6
        bytes[6] = (byte) ((bytes[6] & 0x0F) | 0x70);
        // Set IETF variant (10xx) in the top 2 bits of byte 8
        bytes[8] = (byte) ((bytes[8] & 0x3F) | 0x80);

        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return new UUID(bb.getLong(), bb.getLong());
    }
}
