package io.cresco.filerepo;

import io.cresco.library.crypto.CrescoCrypto;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The two digests filerepo uses. The content hash is the catalog's "md5" field: the library's content hash
 * ({@link CrescoCrypto#CONTENT_HASH_ALG}, what {@code PluginBuilder.getMD5} returns), kept on the wire for
 * compatibility. SHA-384 is the security digest: what a transfer's terminal frame carries and what putjar reports.
 */
final class Hashes {

    static final String SECURITY_ALG = "SHA-384";

    private Hashes() {}

    static MessageDigest sha384() {
        try {
            return MessageDigest.getInstance(SECURITY_ALG);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(SECURITY_ALG + " is unavailable", e);
        }
    }

    static MessageDigest content() {
        try {
            return MessageDigest.getInstance(CrescoCrypto.CONTENT_HASH_ALG);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(CrescoCrypto.CONTENT_HASH_ALG + " is unavailable", e);
        }
    }

    /** {content hash, SHA-384} of a file, lowercase hex, in one read. */
    static String[] both(Path file) throws IOException {
        MessageDigest c = content();
        MessageDigest s = sha384();
        byte[] buf = new byte[1 << 16];
        try (InputStream in = Files.newInputStream(file)) {
            int n;
            while ((n = in.read(buf)) != -1) {
                c.update(buf, 0, n);
                s.update(buf, 0, n);
            }
        }
        return new String[]{hex(c.digest()), hex(s.digest())};
    }

    /** The content hash of bytes in memory, lowercase hex. */
    static String content(byte[] data) {
        return hex(content().digest(data));
    }

    static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
        return sb.toString();
    }
}
