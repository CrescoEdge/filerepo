package io.cresco.filerepo;

import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;

import jakarta.jms.Message;
import jakarta.jms.MessageListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * Several TestRigs on one stand-in fabric: MsgEvents routed to the plugin they name (an RPC's reply returned to the
 * caller, as the controller does), stamped with the sending plugin as their verified caller; dataplane messages
 * published to every listener whose selector matches, each getting its own copy. Frames can be dropped to test loss.
 */
final class Fabric {

    static final String TENANT = "tenant-a";

    private final Gson gson = new Gson();
    private final Map<String, TestRig> rigs = new ConcurrentHashMap<>();
    private final Map<String, Sub> subs = new ConcurrentHashMap<>();
    private final AtomicInteger ids = new AtomicInteger();
    /** Every routed MsgEvent's action, in order. */
    final List<String> routed = new CopyOnWriteArrayList<>();
    /** Dataplane messages to drop (loss): tested before delivery. */
    volatile Predicate<Message> drop = m -> false;
    /** MsgEvents to drop (an unreachable peer). */
    volatile Predicate<MsgEvent> dropRoute = m -> false;

    private static final class Sub {
        final String selector;
        final MessageListener listener;

        Sub(String selector, MessageListener listener) {
            this.selector = selector;
            this.listener = listener;
        }
    }

    void register(TestRig rig) {
        rigs.put(key(TestRig.REGION, TestRig.AGENT, rig.pluginId), rig);
    }

    private static String key(String region, String agent, String plugin) {
        return region + "/" + agent + "/" + plugin;
    }

    String addListener(String selector, MessageListener l) {
        String id = "fabric-listener-" + ids.incrementAndGet();
        subs.put(id, new Sub(selector, l));
        return id;
    }

    void removeListener(String id) {
        subs.remove(id);
    }

    int listenerCount() {
        return subs.size();
    }

    boolean publish(Message m) {
        if (drop.test(m)) return true;
        for (Sub s : new ArrayList<>(subs.values())) {
            if (FakeJms.matches(s.selector, m)) {
                try {
                    s.listener.onMessage(FakeJms.copy(m));
                } catch (RuntimeException ignore) {
                }
            }
        }
        return true;
    }

    /** Deliver {@code msg} from {@code from} to the plugin it names; return an RPC's reply to the caller. */
    void route(TestRig from, MsgEvent msg) {
        routed.add(String.valueOf(msg.getParam("action")));
        if (dropRoute.test(msg)) return;
        TestRig to = rigs.get(key(msg.getDstRegion(), msg.getDstAgent(), msg.getDstPlugin()));
        if (to == null) return;
        MsgEvent copy = gson.fromJson(gson.toJson(msg), MsgEvent.class);
        // infrastructure's stamp: whatever the sender wrote, the caller is the sending plugin
        copy.removeParam(Callers.PARAM_CALLER);
        copy.removeParam(Callers.PARAM_ROLES);
        copy.removeParam("cresco_caller_tenant");
        copy.setParam(Callers.PARAM_CALLER, "plugin:" + TENANT + "/" + TestRig.REGION + "/" + TestRig.AGENT + "/" + from.pluginId);
        copy.setParam("cresco_caller_tenant", TENANT);
        copy.setParam(Callers.PARAM_ROLES, "plugin");
        MsgEvent reply = to.exec.executeEXEC(copy);
        String callId = msg.getParam("callId-" + TestRig.REGION + "-" + TestRig.AGENT + "-" + from.pluginId);
        if (reply != null && callId != null && msg.getParams().containsKey("is_rpc")) {
            from.plugin.receiveRPC(callId, reply);
        }
    }
}
