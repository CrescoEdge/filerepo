package io.cresco.filerepo;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Proofs of the repository's sync_key (HMAC-SHA256), so a mirror takes as producer only a plugin that holds the key an
 * admin provisioned for that repository name, and a keyed producer serves only mirrors that hold it. Every proof binds
 * what it is (discover, reposubscribe, reposubscribed), the repository name, both ends' addresses (each taken from the
 * stamped caller, never from the message), and the fresh values of that exchange (a time, a nonce, a generation), so a
 * proof seen once is no use to anyone else, for anything else, or later.
 */
final class SyncAuth {

    /** How far a discover's time may be from this agent's clock. */
    static final long DISCOVER_SKEW_MS = 10 * 60 * 1000L;

    private SyncAuth() {
    }

    /** The proof of {@code parts}, under {@code key}: hex HMAC-SHA256 of the parts, each length-prefixed. */
    static String mac(byte[] key, String... parts) {
        try {
            Mac m = Mac.getInstance("HmacSHA256");
            m.init(new SecretKeySpec(key, "HmacSHA256"));
            for (String p : parts) {
                byte[] b = String.valueOf(p).getBytes(StandardCharsets.UTF_8);
                m.update((b.length + ":").getBytes(StandardCharsets.US_ASCII));
                m.update(b);
            }
            return Hashes.hex(m.doFinal());
        } catch (Exception ex) {
            throw new IllegalStateException("HmacSHA256 unavailable", ex);
        }
    }

    /** True when {@code proof} is the proof of {@code parts} under {@code key} (compared in constant time). */
    static boolean verify(byte[] key, String proof, String... parts) {
        if (key == null || proof == null) return false;
        return MessageDigest.isEqual(mac(key, parts).getBytes(StandardCharsets.US_ASCII),
                proof.getBytes(StandardCharsets.US_ASCII));
    }

    /** An address as both ends name it in a proof. */
    static String addr(String region, String agent, String plugin) {
        return region + "/" + agent + "/" + plugin;
    }
}
