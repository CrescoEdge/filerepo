package io.cresco.filerepo;

import io.cresco.library.plugin.PluginBuilder;

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
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * The plugin jars in the repository directory, for repolist and getjar. A jar's hash and manifest are read once and
 * kept while its size, modification time and file key are unchanged (the old inventory re-hashed every file in the
 * directory on every getjar and repolist). getjar rechecks the bytes it actually sends against the requested hash, so a
 * file replaced in place without changing those attributes is never served under the old hash.
 */
final class JarInventory {

    /** One jar: its file name, bundle name and version (null when it has no Bundle-SymbolicName), content hash. */
    static final class Entry {
        final String jarfile;
        final String pluginname;
        final String version;
        final String md5;
        final long size;
        final long mtime;
        final Object fileKey;

        Entry(String jarfile, String pluginname, String version, String md5, long size, long mtime, Object fileKey) {
            this.jarfile = jarfile;
            this.pluginname = pluginname;
            this.version = version;
            this.md5 = md5;
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

    private final PluginBuilder plugin;
    private final Map<Path, Entry> cache = new ConcurrentHashMap<>();
    /** jars hashed (cache misses), for tests and diagnostics */
    volatile long hashed;

    JarInventory(PluginBuilder plugin) {
        this.plugin = plugin;
    }

    /** The plugin jars directly in {@code dir} (regular files with a Bundle-SymbolicName; no links, no staging files). */
    List<Entry> list(File dir) {
        List<Entry> out = new ArrayList<>();
        Set<Path> present = new HashSet<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir.toPath())) {
            for (Path p : ds) {
                String name = p.getFileName().toString();
                if (RepoEngine.isStaging(name)) continue;
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
                    if (e == null) continue;
                    cache.put(p, e);
                }
                if (e.pluginname != null) out.add(e);
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
        String md5 = plugin.getMD5(p.toString());
        if (md5 == null) return null;
        hashed++;
        String name = null, version = null;
        try (JarFile jf = new JarFile(p.toFile(), false)) {
            Manifest mf = jf.getManifest();
            if (mf != null) {
                Attributes main = mf.getMainAttributes();
                name = main.getValue("Bundle-SymbolicName");
                version = main.getValue("Bundle-Version");
            }
        } catch (Exception notAJar) {
            // not a jar: listed nowhere, but cached so it is not re-read until it changes
        }
        return new Entry(p.getFileName().toString(), name, version, md5, a.size(), a.lastModifiedTime().toMillis(), a.fileKey());
    }
}
