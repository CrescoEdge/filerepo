package io.cresco.filerepo;

import io.cresco.library.plugin.PluginBuilder;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.Pattern;

/**
 * Where a repository and its catalog live. Nothing here is under the plugin's own data directory, which the controller
 * deletes when it stops a plugin added at runtime (on pluginremove, and before 1.3 hardening on every graceful agent
 * stop): a repository directory and its catalog (the files landed here, the sync state, the producer's identity) must
 * outlive the plugin instance that serves them.
 * <ul>
 *   <li>the repository directory: {@code scan_dir}, else {@code repo_dir}, else
 *   {@code <agent data dir>/filerepo/repos/<filerepo_name, or "default">};</li>
 *   <li>its catalog and lock: {@code <agent data dir>/filerepo/state/<SHA-256 of the directory's REAL path>}, so the
 *   same directory finds the same catalog whatever plugin id serves it and however its path is spelled (a symlinked
 *   parent, /tmp and /private/tmp); and a second lock by the directory's file key (device and inode), under
 *   {@code <agent data dir>/filerepo/locks}, so not even another path to the same directory (a bind mount) opens a
 *   second catalog over it. Two plugins can never serve one directory at once.</li>
 * </ul>
 */
final class RepoPaths {

    private static final Pattern SAFE_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");

    private RepoPaths() {
    }

    static Path agentData(PluginBuilder plugin) {
        return Paths.get(plugin.getAgentService().getAgentDataDirectory()).toAbsolutePath().normalize();
    }

    /** A repository name as one directory name: itself when it is a plain name, otherwise a digest of it. */
    static String dirName(String filerepoName) {
        if (filerepoName == null || filerepoName.isEmpty()) return "default";
        if (SAFE_NAME.matcher(filerepoName).matches() && !filerepoName.contains("..")) return filerepoName;
        return "n-" + DBEngine.sha256(filerepoName).substring(0, 32);
    }

    /** The default repository directory (no scan_dir, no repo_dir): under the agent's data dir, by repository name. */
    static Path defaultRepo(PluginBuilder plugin) {
        return agentData(plugin).resolve("filerepo").resolve("repos")
                .resolve(dirName(plugin.getConfig().getStringParam("filerepo_name")));
    }

    /** The repository directory, absolute: scan_dir, else repo_dir, else the default. */
    static Path repoDir(PluginBuilder plugin) {
        String scan = plugin.getConfig().getStringParam("scan_dir");
        String repo = plugin.getConfig().getStringParam("repo_dir");
        String configured = scan != null ? scan : repo;
        if (configured != null) return Paths.get(configured).toAbsolutePath().normalize();
        return defaultRepo(plugin);
    }

    /**
     * The repository directory's real path (every link resolved). A repository directory that does not exist yet is
     * created first when {@code create} (a mirror's or a landing repository's; never a producer's scan dir, whose
     * absence means something). Null when it cannot be resolved.
     */
    static Path realRepoDir(PluginBuilder plugin, boolean create) {
        Path dir = repoDir(plugin);
        try {
            if (create && !Files.exists(dir)) Files.createDirectories(dir);
            return dir.toRealPath();
        } catch (Exception ex) {
            return null;
        }
    }

    /** The catalog's directory for this repository directory (by its real path; its spelled path if it has none). */
    static Path stateDir(PluginBuilder plugin, Path real) {
        Path key = real != null ? real : repoDir(plugin);
        return agentData(plugin).resolve("filerepo").resolve("state").resolve(DBEngine.sha256(key.toString()).substring(0, 32));
    }

    static Path stateDir(PluginBuilder plugin) {
        return stateDir(plugin, realRepoDir(plugin, false));
    }

    /** The state directory round 2 used (by the spelled path), to be moved to the real path's once. */
    static Path spelledStateDir(PluginBuilder plugin) {
        return agentData(plugin).resolve("filerepo").resolve("state")
                .resolve(DBEngine.sha256(repoDir(plugin).toString()).substring(0, 32));
    }

    /** The lock file for the directory's file key (device and inode), or null where the platform has none. */
    static Path fileKeyLock(PluginBuilder plugin, Path real) {
        if (real == null) return null;
        try {
            Object key = Files.readAttributes(real, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).fileKey();
            if (key == null) return null;
            return agentData(plugin).resolve("filerepo").resolve("locks")
                    .resolve(DBEngine.sha256(key.toString()).substring(0, 32) + ".lock");
        } catch (Exception ex) {
            return null;
        }
    }

    /** Where the catalog was before 1.3 hardening (inside the plugin's data directory). */
    static Path legacyCatalog(PluginBuilder plugin) {
        return Paths.get(plugin.getPluginDataDirectory(), "derbydb-home", "filerepo-db").toAbsolutePath().normalize();
    }

    /** Where the default repository was before (inside the plugin's data directory). */
    static Path legacyDefaultRepo(PluginBuilder plugin) {
        return Paths.get(plugin.getPluginDataDirectory(), "repo").toAbsolutePath().normalize();
    }
}
