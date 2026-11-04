package io.cresco.filerepo;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * The plugin jars in the repository directory, for repolist and getjar. Only a jar that passed signature verification
 * (the agent's plugin-signing policy) is listed or served, whatever door it came in by. A file's manifest, verification
 * and hash are read once and kept while its size, modification time and file key are unchanged; a file the name filter
 * rejects, or that is not a bundle, is never hashed. getjar rechecks the bytes it sends against the requested hash, so
 * a file replaced in place without changing those attributes is never served under the old hash.
 */
final class JarInventory {

    /** Verifies a jar's signature; returns normally to accept it, throws to refuse it. */
    interface Verifier {
        void verify(Path jar) throws Exception;
    }

    /** One file: its name, bundle name and version (null when not a bundle), content hash, and verification. */
    static final class Entry {
        final String jarfile;
        final String pluginname;
        final String version;
        final String md5;
        final boolean verified;
        final long size;
        final long mtime;
        final Object fileKey;

        Entry(String jarfile, String pluginname, String version, String md5, boolean verified, long size, long mtime,
              Object fileKey) {
            this.jarfile = jarfile;
            this.pluginname = pluginname;
            this.version = version;
            this.md5 = md5;
            this.verified = verified;
            this.size = size;
            this.mtime = mtime;
            this.fileKey = fileKey;
        }

        boolean sameFile(BasicFileAttributes a) {
            return a.size() == size && a.lastModifiedTime().toMillis() == mtime && Objects.equals(a.fileKey(), fileKey);
        }

        Map<String, String> asMap() {
            Map<String, String> m = new HashMap<>();
            m.put("pluginname", pluginname);
            m.put("jarfile", jarfile);
            m.put("md5", md5);
            m.put("version", version);
            return m;
        }
    }

    private final Predicate<String> names;
    private final Verifier verifier;
    private final Map<Path, Entry> cache = new ConcurrentHashMap<>();
    /** files hashed (cache misses of bundles), for tests and diagnostics */
    volatile long hashed;

    JarInventory(Predicate<String> names, Verifier verifier) {
        this.names = names;
        this.verifier = verifier;
    }

    /** The verified plugin jars directly in {@code dir}: regular files, no links, no staging files. */
    List<Entry> list(File dir) {
        List<Entry> out = new ArrayList<>();
        Set<Path> present = new HashSet<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir.toPath())) {
            for (Path p : ds) {
                String name = p.getFileName().toString();
                if (RepoEngine.isStaging(name) || !names.test(name)) continue;
                BasicFileAttributes a;
                try {
                    a = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                } catch (IOException gone) {
                    continue;
                }
                if (!a.isRegularFile()) continue;
                present.add(p);
                Entry e = cache.get(p);
                if (e == null || !e.sameFile(a)) {
                    e = read(p, a);
                    cache.put(p, e);
                }
                if (e.pluginname != null && e.verified && e.md5 != null) out.add(e);
            }
        } catch (IOException ex) {
            return out;
        }
        cache.keySet().removeIf(p -> p.startsWith(dir.toPath()) && !present.contains(p));
        return out;
    }

    /** Forget a jar (it changed under its cached attributes). */
    void invalidate(Path p) {
        cache.remove(p);
    }

    private Entry read(Path p, BasicFileAttributes a) {
        String name = null, version = null;
        try (JarFile jf = new JarFile(p.toFile(), false)) {
            Manifest mf = jf.getManifest();
            if (mf != null) {
                Attributes main = mf.getMainAttributes();
                name = main.getValue("Bundle-SymbolicName");
                version = main.getValue("Bundle-Version");
            }
        } catch (Exception notAJar) {
            // not a jar: listed nowhere, cached so it is not re-read until it changes
        }
        if (name == null) {
            return new Entry(p.getFileName().toString(), null, null, null, false, a.size(), a.lastModifiedTime().toMillis(), a.fileKey());
        }
        boolean verified;
        try {
            verifier.verify(p);
            verified = true;
        } catch (Exception refused) {
            verified = false;
        }
        String md5 = verified ? contentHash(p) : null;
        if (md5 != null) hashed++;
        return new Entry(p.getFileName().toString(), name, version, md5, verified, a.size(), a.lastModifiedTime().toMillis(), a.fileKey());
    }

    static String contentHash(Path p) {
        return Hashes.content(p);
    }

    static String hex(byte[] d) {
        return Hashes.hex(d);
    }
}
