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

    public FileRepoHealthCheck(PluginBuilder plugin, RepoEngine repoEngine) {
        this.plugin = plugin;
        this.repoEngine = repoEngine;
    }

    @Override
    public Result execute() {
        try {
            if (plugin == null || !plugin.isActive() || repoEngine == null) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "filerepo not active");
            }
            long files = repoEngine.getRepoCount();
            File repoDir = repoEngine.getRepoDir();
            if (repoDir != null && repoDir.isDirectory() && !repoDir.canWrite()) {
                return new Result(Result.Status.WARN,
                        "filerepo WARN: repo dir not writable: " + repoDir.getAbsolutePath());
            }
            String where = (repoDir != null) ? repoDir.getName() : "unset";
            return new Result(Result.Status.OK,
                    "filerepo OK: " + files + " file(s) cataloged, repo dir=" + where);
        } catch (Exception ex) {
            return new Result(Result.Status.WARN, "filerepo health error: " + ex.getMessage());
        }
    }
}
