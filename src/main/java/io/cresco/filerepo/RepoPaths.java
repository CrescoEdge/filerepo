package io.cresco.filerepo;

import io.cresco.library.plugin.PluginBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * Where a repository and its catalog live. Nothing here is under the plugin's own data directory, which the controller
 * deletes when it stops a plugin added at runtime (on pluginremove, and before 1.3 hardening on every graceful agent
 * stop): a repository directory and its catalog (the files landed here, the sync state, the producer's identity) must
 * outlive the plugin instance that serves them.
 * <ul>
 *   <li>the repository directory: {@code scan_dir}, else {@code repo_dir}, else
 *   {@code <agent data dir>/filerepo/repos/<filerepo_name, or "default">};</li>
 *   <li>its catalog and lock: {@code <agent data dir>/filerepo/state/<SHA-256 of the repository directory>}, so the same
 *   directory finds the same catalog whatever plugin id serves it (a redeployed plugin keeps its identity and sync
 *   state), and two plugins can never serve one directory at once.</li>
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

    /** The catalog's directory for this repository directory. */
    static Path stateDir(PluginBuilder plugin) {
        return agentData(plugin).resolve("filerepo").resolve("state")
                .resolve(DBEngine.sha256(repoDir(plugin).toString()).substring(0, 32));
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
