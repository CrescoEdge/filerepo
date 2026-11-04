package io.cresco.filerepo;

import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;

/**
 * The verified caller of a message, as infrastructure stamped it (controller docs/caller-authorization.md; the
 * library's {@link Caller}). A message's own source fields are written by its sender and are never read for identity.
 * A message with no stamp is anonymous (every role) only in explicit insecure development mode
 * ({@code broker_security_enabled=false} given to the agent as -D or CRESCO_ environment); otherwise it has no caller
 * and every decision that needs one refuses. A malformed stamp is never trusted.
 */
final class Callers {

    static final String PARAM_CALLER = Caller.PARAM_CALLER;
    static final String PARAM_ROLES = Caller.PARAM_ROLES;
    static final String ROLE_ADMIN = Caller.ROLE_ADMIN;

    private Callers() {}

    /** True when the agent was started with {@code broker_security_enabled=false} (-D or CRESCO_ environment). */
    static boolean insecureMode(io.cresco.library.plugin.PluginBuilder plugin) {
        // the agent's own decision (broker_security_enabled=false set explicitly); the property only on an agent whose
        // library predates AgentService.isSecurityEnabled
        try {
            return !plugin.getAgentService().isSecurityEnabled();
        } catch (NoSuchMethodError | AbstractMethodError olderLibrary) {
            return "false".equalsIgnoreCase(System.getProperty("broker_security_enabled"))
                    || "false".equalsIgnoreCase(System.getenv("CRESCO_broker_security_enabled"));
        }
    }

    /** The verified caller; with no stamp, anonymous in insecure mode, otherwise null. */
    static Caller of(MsgEvent m, boolean insecure) {
        if (m == null) return null;
        Caller c = Caller.fromMsgEvent(m);
        if (c != null) return c;
        if (Caller.hasAny(m)) return null;
        return insecure ? Caller.anonymous() : null;
    }

    /** True when the message's verified caller holds {@code role}. No caller: false (fail closed). */
    static boolean hasRole(MsgEvent m, String role, boolean insecure) {
        Caller c = of(m, insecure);
        return c != null && c.hasRole(role);
    }

    /**
     * Who to count and own a request as: the verified caller's principal; in insecure mode an unstamped caller is
     * "anonymous" (with the message's source as a development-only fairness bucket). Null: no verified caller.
     */
    static String key(MsgEvent m, boolean insecure) {
        Caller c = of(m, insecure);
        if (c == null) return null;
        if (c.isAnonymous()) return "anonymous:" + m.getSrcRegion() + "/" + m.getSrcAgent() + "/" + m.getSrcPlugin();
        return c.getPrincipal();
    }

    /** The verified caller's tenant ("-" for an anonymous caller in insecure mode), or null. */
    static String tenant(MsgEvent m, boolean insecure) {
        Caller c = of(m, insecure);
        if (c == null) return null;
        if (c.isAnonymous() || c.getTenant() == null) return "-";
        return c.getTenant();
    }

    /**
     * The plugin a sync message comes from, {region, agent, pluginid}, as the verified caller names it. Null when the
     * caller is not verified, or not a plugin. In insecure mode an anonymous caller is addressed by the message's
     * source fields (development only: nothing is verified then).
     */
    static String[] plugin(MsgEvent m, boolean insecure) {
        Caller c = of(m, insecure);
        if (c == null) return null;
        if (c.isAnonymous()) return new String[]{m.getSrcRegion(), m.getSrcAgent(), m.getSrcPlugin()};
        if (c.getKind() != Caller.Kind.PLUGIN) return null;
        return new String[]{c.getRegion(), c.getAgent(), c.getPluginId()};
    }
}
