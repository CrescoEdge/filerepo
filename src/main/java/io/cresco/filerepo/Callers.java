package io.cresco.filerepo;

import io.cresco.library.messaging.MsgEvent;

/**
 * The verified caller of a message, as infrastructure stamped it (controller docs/caller-authorization.md): the
 * reserved parameters {@code cresco_caller} and {@code cresco_caller_roles}. A sender can never set them. Action-level
 * roles are enforced before filerepo runs; this answers the parameter-level questions an action asks itself (may this
 * caller force a clear?) and names a caller for per-caller bounds.
 */
final class Callers {

    static final String PARAM_CALLER = "cresco_caller";
    static final String PARAM_ROLES = "cresco_caller_roles";
    static final String ROLE_ADMIN = "admin";

    private Callers() {}

    /** True when the message's verified caller holds {@code role}. No caller: false (fail closed). */
    static boolean hasRole(MsgEvent m, String role) {
        String principal = m.getParam(PARAM_CALLER);
        String roles = m.getParam(PARAM_ROLES);
        if (principal == null || principal.isEmpty() || roles == null) return false;
        for (String r : roles.split(",")) {
            if (r.trim().equals(role)) return true;
        }
        return false;
    }

    /** Who to count a request against: the verified caller, or, without one, the message's source. */
    static String key(MsgEvent m) {
        String principal = m.getParam(PARAM_CALLER);
        if (principal != null && !principal.isEmpty()) return principal;
        return "src:" + m.getSrcRegion() + "/" + m.getSrcAgent() + "/" + m.getSrcPlugin();
    }
}
