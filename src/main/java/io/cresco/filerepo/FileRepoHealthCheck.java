package io.cresco.filerepo;

import io.cresco.library.plugin.PluginBuilder;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

import java.io.File;

/**
 * Central health for filerepo. Registered as an {@code org.apache.felix.hc.api.HealthCheck} OSGi
 * service (name "filerepo", tag "local") so the controller's CrescoHealthExecutor discovers and
 * schedules it alongside the built-in broker/db/disk/memory/plugins checks — the same Felix Health
 * Check system every other Cresco bundle uses. Reports the catalog size and verifies the repo
 * directory is writable; self-guards while the plugin is still coming up.
 */
public class FileRepoHealthCheck implements HealthCheck {

    private final PluginBuilder plugin;
    private final RepoEngine repoEngine;
    private final LiveTierClient liveTier;

    public FileRepoHealthCheck(PluginBuilder plugin, RepoEngine repoEngine, LiveTierClient liveTier) {
        this.plugin = plugin;
        this.repoEngine = repoEngine;
        this.liveTier = liveTier;
    }

    @Override
    public Result execute() {
        try {
            if (plugin == null || !plugin.isActive() || repoEngine == null) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "filerepo not active");
            }
            // A dead catalog (XSDB6 double-boot, missing FILELIST table, corrupted Derby) used to
            // read as "0 file(s) cataloged, OK" because getRepoCount() swallows errors and returns 0.
            if (!repoEngine.isCatalogHealthy()) {
                return new Result(Result.Status.CRITICAL, "filerepo CRITICAL: catalog not queryable");
            }
            if (repoEngine.isLiveBacked()) {
                // no repo dir of its own to check: its files are the live row's, read only through the filesystem.
                // No provider call here: whether one is tracked is enough, and costs nothing.
                if (liveTier == null || !liveTier.present()) {
                    return new Result(Result.Status.WARN, "filerepo WARN: live-backed repository (row "
                            + repoEngine.getLiveRow() + "): no live-tier provider admits this bundle");
                }
                return new Result(Result.Status.OK, "filerepo OK: live-backed repository (row "
                        + repoEngine.getLiveRow() + "), live-tier provider present");
            }
            File repoDir = repoEngine.getRepoDir();
            // An absent/uncreatable repo dir must be CRITICAL, not skipped into an OK result.
            if (repoDir == null || !repoDir.isDirectory()) {
                return new Result(Result.Status.CRITICAL, "filerepo CRITICAL: repo dir missing/uncreatable");
            }
            if (!repoDir.canWrite()) {
                return new Result(Result.Status.WARN,
                        "filerepo WARN: repo dir not writable: " + repoDir.getAbsolutePath());
            }
            long files = repoEngine.getRepoCount();
            return new Result(Result.Status.OK,
                    "filerepo OK: " + files + " file(s) cataloged, repo dir=" + repoDir.getName());
        } catch (Exception ex) {
            return new Result(Result.Status.WARN, "filerepo health error: " + ex.getMessage());
        }
    }
}
