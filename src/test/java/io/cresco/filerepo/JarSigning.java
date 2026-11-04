package io.cresco.filerepo;

import jdk.security.jarsigner.JarSigner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSigner;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertPath;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipFile;

/**
 * Test-time plugin signing: a CA and a code-signing leaf it issued, made once per test JVM with the JDK's own keytool in
 * a temporary directory (random store passwords; nothing is committed), a trust store holding the CA, and a second,
 * untrusted CA. Jars are built and signed in memory with the JDK's JarSigner.
 */
final class JarSigning {

    private static Path dir;
    private static char[] pass;
    private static KeyStore trust;
    private static PrivateKey trustedKey, foreignKey;
    private static CertPath trustedPath, foreignPath;
    private static X509Certificate ca;

    private JarSigning() {}

    static synchronized void init() throws Exception {
        if (dir != null) return;
        dir = Files.createTempDirectory("filerepo-signing");
        dir.toFile().deleteOnExit();
        byte[] r = new byte[18];
        new SecureRandom().nextBytes(r);
        pass = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(r).toCharArray();

        // trusted CA and a leaf it issued for code signing
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp384r1", "-sigalg", "SHA384withECDSA",
                "-dname", "CN=filerepo test plugin CA", "-ext", "bc:c", "-validity", "2", "-keystore", p("ca.p12"));
        keytool("-genkeypair", "-alias", "signer", "-keyalg", "EC", "-groupname", "secp384r1", "-sigalg", "SHA384withECDSA",
                "-dname", "CN=filerepo test plugin signer", "-validity", "2", "-keystore", p("signer.p12"));
        keytool("-certreq", "-alias", "signer", "-keystore", p("signer.p12"), "-file", p("signer.csr"));
        keytool("-gencert", "-alias", "ca", "-keystore", p("ca.p12"), "-infile", p("signer.csr"), "-outfile", p("signer.crt"),
                "-ext", "ku:c=digitalSignature", "-ext", "eku=codeSigning", "-validity", "2", "-sigalg", "SHA384withECDSA");
        keytool("-exportcert", "-alias", "ca", "-keystore", p("ca.p12"), "-file", p("ca.crt"));
        keytool("-importcert", "-noprompt", "-alias", "ca", "-keystore", p("signer.p12"), "-file", p("ca.crt"));
        keytool("-importcert", "-noprompt", "-alias", "signer", "-keystore", p("signer.p12"), "-file", p("signer.crt"));
        keytool("-importcert", "-noprompt", "-alias", "plugin-ca", "-keystore", p("trust.p12"), "-file", p("ca.crt"));
        // a foreign, self-signed signer nobody trusts
        keytool("-genkeypair", "-alias", "foreign", "-keyalg", "EC", "-groupname", "secp384r1", "-sigalg", "SHA384withECDSA",
                "-dname", "CN=someone else", "-validity", "2", "-keystore", p("foreign.p12"));

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        KeyStore signer = load(p("signer.p12"));
        trustedKey = (PrivateKey) signer.getKey("signer", pass);
        List<Certificate> chain = new ArrayList<>(Arrays.asList(signer.getCertificateChain("signer")));
        trustedPath = cf.generateCertPath(chain);
        KeyStore foreign = load(p("foreign.p12"));
        foreignKey = (PrivateKey) foreign.getKey("foreign", pass);
        foreignPath = cf.generateCertPath(Arrays.asList(foreign.getCertificateChain("foreign")));
        trust = load(p("trust.p12"));
        ca = (X509Certificate) trust.getCertificate("plugin-ca");
    }

    private static String p(String name) {
        return dir.resolve(name).toString();
    }

    private static KeyStore load(String path) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            ks.load(in, pass);
        }
        return ks;
    }

    private static void keytool(String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "keytool");
        cmd.addAll(Arrays.asList(args));
        cmd.add("-storetype");
        cmd.add("PKCS12");
        cmd.add("-storepass");
        cmd.add(new String(pass));
        if (Arrays.asList(args).contains("-genkeypair")) {
            cmd.add("-keypass");
            cmd.add(new String(pass));
        }
        Process proc = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        byte[] out = proc.getInputStream().readAllBytes();
        if (proc.waitFor() != 0) {
            throw new IllegalStateException("keytool " + args[0] + " failed: " + new String(out, StandardCharsets.UTF_8));
        }
    }

    /** The trust store (PKCS12) holding the test CA, and its password. */
    static KeyStore trustStore() throws Exception {
        init();
        return trust;
    }

    static Path trustStorePath() throws Exception {
        init();
        return dir.resolve("trust.p12");
    }

    static char[] password() throws Exception {
        init();
        return pass.clone();
    }

    /** An unsigned bundle jar named {@code symbolicName} with one class-like entry. */
    static byte[] unsignedJar(String symbolicName, String content) throws Exception {
        Manifest mf = new Manifest();
        Attributes main = mf.getMainAttributes();
        main.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        main.putValue("Bundle-SymbolicName", symbolicName);
        main.putValue("Bundle-Version", "1.3.0");
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (JarOutputStream jos = new JarOutputStream(bos, mf)) {
            jos.putNextEntry(new JarEntry("io/cresco/test/Payload.txt"));
            jos.write(content.getBytes(StandardCharsets.UTF_8));
            jos.closeEntry();
        }
        return bos.toByteArray();
    }

    /** {@code symbolicName}'s jar, signed by the trusted leaf. */
    static byte[] signedJar(String symbolicName, String content) throws Exception {
        init();
        return sign(unsignedJar(symbolicName, content), trustedKey, trustedPath);
    }

    /** {@code symbolicName}'s jar, signed by a signer the trust store does not hold. */
    static byte[] foreignSignedJar(String symbolicName, String content) throws Exception {
        init();
        return sign(unsignedJar(symbolicName, content), foreignKey, foreignPath);
    }

    private static byte[] sign(byte[] jar, PrivateKey key, CertPath path) throws Exception {
        Path in = Files.createTempFile(dir, "unsigned", ".jar");
        Files.write(in, jar);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ZipFile zf = new ZipFile(in.toFile()); OutputStream out = bos) {
            JarSigner.Builder b = new JarSigner.Builder(key, path).digestAlgorithm("SHA-384")
                    .signatureAlgorithm("SHA384withECDSA").signerName("FILEREPO");
            b.build().sign(zf, out);
        } finally {
            Files.deleteIfExists(in);
        }
        return bos.toByteArray();
    }

}
