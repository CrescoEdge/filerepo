package io.cresco.filerepo;

import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.security.Caller;

/**
 * The verified caller of a message, as infrastructure stamped it (controller docs/caller-authorization.md; the
 * library's {@link Caller}). A sender can never set it. Action-level roles ({@code @CrescoAction(roles=...)}) are
 * enforced before filerepo runs; this answers the parameter-level questions an action asks itself (may this caller force
 * a clear?) and names a caller for per-caller bounds and identity checks.
 */
final class Callers {

    static final String PARAM_CALLER = Caller.PARAM_CALLER;
    static final String PARAM_ROLES = Caller.PARAM_ROLES;
    static final String ROLE_ADMIN = Caller.ROLE_ADMIN;

    private Callers() {}

    /** The verified caller, or null when the message carries none (or a malformed one). */
    static Caller of(MsgEvent m) {
        return Caller.fromMsgEvent(m);
    }

    /** True when the message's verified caller holds {@code role}. No caller: false (fail closed). */
    static boolean hasRole(MsgEvent m, String role) {
        Caller c = of(m);
        return c != null && c.hasRole(role);
    }

    /** Who to count a request against: the verified caller, or, without one (or anonymous), the message's source. */
    static String key(MsgEvent m) {
        Caller c = of(m);
        String src = m.getSrcRegion() + "/" + m.getSrcAgent() + "/" + m.getSrcPlugin();
        if (c == null) return "src:" + src;
        if (c.isAnonymous()) return "anonymous:" + src;
        return c.getPrincipal();
    }

    /**
     * The plugin a sync message comes from: {region, agent, pluginid} of the verified plugin caller, or of the message's
     * source when no caller is stamped (an older controller) or the caller is anonymous (development). Null when the
     * verified caller is not a plugin: then it is not another filerepo.
     */
    static String[] plugin(MsgEvent m) {
        Caller c = of(m);
        if (c == null || c.isAnonymous()) return new String[]{m.getSrcRegion(), m.getSrcAgent(), m.getSrcPlugin()};
        if (c.getKind() != Caller.Kind.PLUGIN) return null;
        return new String[]{c.getRegion(), c.getAgent(), c.getPluginId()};
    }
}
