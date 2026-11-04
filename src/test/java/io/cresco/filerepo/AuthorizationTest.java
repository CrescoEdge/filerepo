package io.cresco.filerepo;

import io.cresco.library.capability.CrescoAction;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * filerepo's side of the caller-authorization contract (controller docs/caller-authorization.md): the roles each action
 * declares for PluginBuilder.msgIn to enforce, and the identity checks the sync verbs make themselves from the verified
 * caller, never from a message's source fields.
 */
class AuthorizationTest {

    @TempDir Path tmp;
    TestRig rig;

    @AfterEach
    void down() {
        if (rig != null) rig.close();
    }

    static Map<String, Set<String>> declaredRoles() {
        Map<String, Set<String>> out = new TreeMap<>();
        for (CrescoAction a : ExecutorImpl.class.getAnnotationsByType(CrescoAction.class)) {
            out.put(a.name(), new HashSet<>(Arrays.asList(a.roles())));
        }
        return out;
    }

    @Test
    void theContractsPrivilegedActionsNeedInfraOrAdminAndNothingElse() {
        Map<String, Set<String>> roles = declaredRoles();
        Set<String> privileged = Set.of("infra", "admin");
        // the contract's list: none may be removed
        for (String action : new String[]{"clearrepo", "putjar", "removefile", "putfiles"}) {
            assertEquals(privileged, roles.get(action), action);
        }
        // added by filerepo: pushing local files anywhere, and releasing held deletes
        assertEquals(privileged, roles.get("putfilesremote"));
        assertEquals(privileged, roles.get("releasedeletes"));
        assertEquals(privileged, roles.get("releaserewrites"));
        // a lease extension as the node itself
        assertEquals(privileged, roles.get("liverealise"));
        // sync messages come from another filerepo
        for (String sync : new String[]{"repolistin", "repoconfirm", "reposubscribe", "reposubscribed"}) {
            assertEquals(Set.of("plugin"), roles.get(sync), sync);
        }
        // everything else: the default (same tenant, and infra)
        for (String open : new String[]{"repolist", "getrepofilelist", "getfile", "getjar", "streamfile",
                "streamfilecancel", "getscandir", "repostate", "liverelay", "getmetrics", "getcapabilities"}) {
            assertEquals(Set.of(), roles.get(open), open);
        }
        assertEquals(23, roles.size(), "every action is declared: " + roles.keySet());
    }

    @Test
    void aDiffIsTakenFromTheVerifiedCallerNotTheSourceFields() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("filerepo_name", "fr");
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("mirror")).toString());
        rig = new TestRig(tmp.resolve("agent"), cfg, null);
        rig.repo.rememberProducer(TestRig.REGION, TestRig.AGENT, "real-producer");

        // the source fields name the real producer, but the stamped caller is another plugin
        MsgEvent spoofed = new MsgEvent(MsgEvent.Type.EXEC, TestRig.REGION, TestRig.AGENT, "real-producer",
                TestRig.REGION, TestRig.AGENT, TestRig.PLUGIN_ID, false, false);
        spoofed.setParam("action", "repolistin");
        spoofed.setParam("transfer_id", "1");
        spoofed.setCompressedParam("repolistin", "{}");
        spoofed.setParam(Callers.PARAM_CALLER, "plugin:tenant-a/" + TestRig.REGION + "/" + TestRig.AGENT + "/impostor");
        spoofed.setParam("cresco_caller_tenant", "tenant-a");
        spoofed.setParam(Callers.PARAM_ROLES, "plugin");
        assertEquals("9", rig.call(spoofed).getParam("status_code"), "the impostor is not a subscribed producer");

        // an API user is not a filerepo at all
        MsgEvent api = TestRig.as(TestRig.request("repolistin", "transfer_id", "2"), "admin");
        api.setCompressedParam("repolistin", "{}");
        MsgEvent r = rig.call(api);
        assertEquals("9", r.getParam("status_code"));
        assertTrue(r.getParam("status_desc").contains("verified filerepo plugin caller"), r.getParam("status_desc"));

        // the real producer, verified, is accepted
        MsgEvent real = new MsgEvent(MsgEvent.Type.EXEC, TestRig.REGION, TestRig.AGENT, "anything",
                TestRig.REGION, TestRig.AGENT, TestRig.PLUGIN_ID, false, false);
        real.setParam("action", "repolistin");
        real.setParam("transfer_id", "3");
        real.setCompressedParam("repolistin", "{}");
        real.setParam("sync_gen", rig.repo.getSyncGen());
        real.setParam(Callers.PARAM_CALLER, "plugin:tenant-a/" + TestRig.REGION + "/" + TestRig.AGENT + "/real-producer");
        real.setParam("cresco_caller_tenant", "tenant-a");
        real.setParam(Callers.PARAM_ROLES, "plugin");
        assertEquals("10", rig.call(real).getParam("status_code"));
    }

    @Test
    void callersAreNamedByTheirVerifiedStamp() throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("repo_dir", Files.createDirectories(tmp.resolve("repo")).toString());
        rig = new TestRig(tmp.resolve("agent"), cfg, null);
        MsgEvent a = TestRig.as(TestRig.request("x"), "user");
        MsgEvent b = TestRig.request("x");
        assertEquals("api:tenant-a/operator", Callers.key(a, false));
        assertNull(Callers.key(b, false), "no stamp, secure: no caller (the source fields are never an identity)");
        assertNull(Callers.plugin(b, false));
        assertEquals("anonymous:" + TestRig.REGION + "/" + TestRig.AGENT + "/caller", Callers.key(b, true),
                "no stamp in explicit insecure mode: anonymous");
        assertTrue(Callers.hasRole(b, "admin", true), "insecure mode: every role");
        assertFalse(Callers.hasRole(b, "admin", false));
        MsgEvent forged = TestRig.request("x");
        forged.setParam(Callers.PARAM_CALLER, "not a principal");
        forged.setParam(Callers.PARAM_ROLES, "admin");
        assertFalse(Callers.hasRole(forged, "admin", false), "a malformed stamp grants nothing");
        assertFalse(Callers.hasRole(forged, "admin", true), "not even in insecure mode: a malformed stamp is never trusted");
    }
}
