package io.cresco.filerepo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;

/**
 * A producer's own signing key: an EC P-384 key pair (SHA384withECDSA, the agent's CNSA policy), made once with the JDK
 * and kept owner-only in the repository's state directory, which one plugin at a time holds (the real-path and file-key
 * lock). A producer's identity to its mirrors is the node its caller stamp names plus this key's fingerprint (the
 * SHA-256 of its SubjectPublicKeyInfo): a producer redeployed over the same directory finds the same key; another
 * plugin on the same agent, which cannot open that state directory through filerepo, cannot sign as it.
 *
 * <p>In-JVM boundary: code running in the agent's JVM can read any file the agent can (another bundle reading this key
 * file is in-process code, which the caller-authorization contract treats as trusted: only signed plugins run there).
 * The key protects against every other node and every plugin that is not trusted code of this agent.
 */
final class ProducerKey {

    static final String FILE = "producer.key";
    static final String SIG_ALG = "SHA384withECDSA";
    static final String CURVE = "secp384r1";

    final PrivateKey privateKey;
    final PublicKey publicKey;
    /** SHA-256 of the SubjectPublicKeyInfo, lowercase hex (64). */
    final String fingerprint;
    /** The SubjectPublicKeyInfo, base64. */
    final String publicB64;

    private ProducerKey(PrivateKey priv, PublicKey pub) {
        this.privateKey = priv;
        this.publicKey = pub;
        this.fingerprint = fingerprint(pub.getEncoded());
        this.publicB64 = Base64.getEncoder().encodeToString(pub.getEncoded());
    }

    /** What loading found, for the log: made now, loaded, or loaded with permissions tightened. */
    static final class Loaded {
        final ProducerKey key;
        final boolean created;
        final boolean tightened;

        Loaded(ProducerKey key, boolean created, boolean tightened) {
            this.key = key;
            this.created = created;
            this.tightened = tightened;
        }
    }

    /**
     * The key kept in {@code stateDir}, made (owner-only, written to a temporary file and renamed into place) when there
     * is none. A key file that is a link, not a regular file, or unreadable is refused (never replaced: the producer's
     * identity is that key, and a new one would need every mirror re-pinned). A key file others may read is made
     * owner-only again (and said so: it may have been copied).
     */
    static Loaded loadOrCreate(Path stateDir) throws IOException, GeneralSecurityException {
        Path file = stateDir.resolve(FILE);
        if (Files.exists(file, LinkOption.NOFOLLOW_LINKS)) {
            if (Files.isSymbolicLink(file) || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException(file + " is not a regular file");
            }
            boolean tightened = false;
            try {
                Set<PosixFilePermission> perms = Files.getPosixFilePermissions(file, LinkOption.NOFOLLOW_LINKS);
                Set<PosixFilePermission> others = EnumSet.of(PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE,
                        PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE,
                        PosixFilePermission.OTHERS_EXECUTE);
                others.retainAll(perms);
                if (!others.isEmpty()) {
                    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
                    tightened = true;
                }
            } catch (UnsupportedOperationException noPosix) {
                // not a POSIX filesystem
            }
            return new Loaded(read(file), false, tightened);
        }
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec(CURVE));
        KeyPair kp = g.generateKeyPair();
        Properties p = new Properties();
        p.setProperty("alg", "EC-P384");
        p.setProperty("private", Base64.getEncoder().encodeToString(kp.getPrivate().getEncoded()));
        p.setProperty("public", Base64.getEncoder().encodeToString(kp.getPublic().getEncoded()));
        Path tmp = stateDir.resolve(FILE + ".tmp");
        Files.deleteIfExists(tmp);
        try {
            Files.createFile(tmp, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        } catch (UnsupportedOperationException noPosix) {
            Files.createFile(tmp);
        }
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(tmp, StandardOpenOption.WRITE)) {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            p.store(out, "filerepo producer signing key: the producer's identity to its mirrors. Keep it owner-only; back"
                    + " it up with the state directory");
            ch.write(java.nio.ByteBuffer.wrap(out.toByteArray()));
            ch.force(true);
        }
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE);
        return new Loaded(read(file), true, false);
    }

    private static ProducerKey read(Path file) throws IOException, GeneralSecurityException {
        Properties p = new Properties();
        try (java.io.InputStream in = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
            p.load(in);
        }
        String priv = p.getProperty("private"), pub = p.getProperty("public");
        if (priv == null || pub == null) throw new IOException(file + " holds no key pair");
        KeyFactory kf = KeyFactory.getInstance("EC");
        PrivateKey sk = kf.generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(priv.trim())));
        PublicKey pk = kf.generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(pub.trim())));
        ProducerKey k = new ProducerKey(sk, pk);
        // the two halves belong together (a file edited by hand, or two halves of different keys, is refused)
        if (!verify(pk, k.sign("filerepo-key-check"), "filerepo-key-check")) {
            throw new GeneralSecurityException(file + ": its private and public keys do not match");
        }
        return k;
    }

    /** A signature (base64) over {@code parts}, each length-prefixed. */
    String sign(String... parts) {
        try {
            Signature s = Signature.getInstance(SIG_ALG);
            s.initSign(privateKey);
            feed(s, parts);
            return Base64.getEncoder().encodeToString(s.sign());
        } catch (GeneralSecurityException ex) {
            throw new IllegalStateException(SIG_ALG + " signing failed", ex);
        }
    }

    /** True when {@code sig} (base64) is {@code key}'s signature over {@code parts}. */
    static boolean verify(PublicKey key, String sig, String... parts) {
        if (key == null || sig == null || sig.isEmpty() || sig.length() > 1024) return false;
        try {
            Signature s = Signature.getInstance(SIG_ALG);
            s.initVerify(key);
            feed(s, parts);
            return s.verify(Base64.getDecoder().decode(sig));
        } catch (GeneralSecurityException | IllegalArgumentException ex) {
            return false;
        }
    }

    private static void feed(Signature s, String... parts) throws GeneralSecurityException {
        for (String part : parts) {
            byte[] b = String.valueOf(part).getBytes(StandardCharsets.UTF_8);
            s.update((b.length + ":").getBytes(StandardCharsets.US_ASCII));
            s.update(b);
        }
    }

    /** A producer's public key from its SubjectPublicKeyInfo (base64): an EC key on P-384 only; null otherwise. */
    static PublicKey publicKey(String b64) {
        if (b64 == null || b64.isEmpty() || b64.length() > 4096) return null;
        try {
            PublicKey k = KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(b64)));
            if (!(k instanceof ECPublicKey) || ((ECPublicKey) k).getParams().getCurve().getField().getFieldSize() != 384) {
                return null;
            }
            return k;
        } catch (GeneralSecurityException | IllegalArgumentException ex) {
            return null;
        }
    }

    /** SHA-256 of a SubjectPublicKeyInfo, lowercase hex. */
    static String fingerprint(byte[] spki) {
        try {
            return Hashes.hex(MessageDigest.getInstance("SHA-256").digest(spki));
        } catch (GeneralSecurityException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /** SHA-256 of a string (UTF-8), lowercase hex: what a signature binds of a large payload. */
    static String digest(String s) {
        return DBEngine.sha256(s == null ? "" : s);
    }
}
