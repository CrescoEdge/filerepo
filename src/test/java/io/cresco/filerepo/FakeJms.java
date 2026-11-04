package io.cresco.filerepo;

import jakarta.jms.BytesMessage;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JMS messages for tests: string, boolean and long properties, a bytes body (written, then read from the start), a
 * text body, a map body. {@link #copy} gives each subscriber its own message to read, as a broker does.
 */
final class FakeJms {

    private FakeJms() {}

    /** The state behind one fake message. */
    static final class State implements InvocationHandler {
        final Class<?> type;
        final Map<String, Object> props = new LinkedHashMap<>();
        final Map<String, Object> map = new LinkedHashMap<>();
        final ByteArrayOutputStream written = new ByteArrayOutputStream();
        byte[] body;
        int readPos;
        String text;

        State(Class<?> type) {
            this.type = type;
        }

        private byte[] bytes() {
            if (body == null) body = written.toByteArray();
            return body;
        }

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method m, Object[] a) {
            String n = m.getName();
            switch (n) {
                case "setStringProperty": case "setObjectProperty": case "setBooleanProperty":
                case "setLongProperty": case "setIntProperty":
                    props.put((String) a[0], a[1]);
                    return null;
                case "getStringProperty":
                    return props.get(a[0]) == null ? null : String.valueOf(props.get(a[0]));
                case "getObjectProperty":
                    return props.get(a[0]);
                case "getBooleanProperty":
                    return Boolean.parseBoolean(String.valueOf(props.get(a[0])));
                case "getLongProperty":
                    return Long.parseLong(String.valueOf(props.get(a[0])));
                case "getIntProperty":
                    return Integer.parseInt(String.valueOf(props.get(a[0])));
                case "propertyExists":
                    return props.containsKey(a[0]);
                case "getPropertyNames":
                    return Collections.enumeration(props.keySet());
                case "writeBytes":
                    if (a.length == 1) written.write((byte[]) a[0], 0, ((byte[]) a[0]).length);
                    else written.write((byte[]) a[0], (Integer) a[1], (Integer) a[2]);
                    body = null;
                    return null;
                case "getBodyLength":
                    return (long) bytes().length;
                case "readBytes": {
                    byte[] dst = (byte[]) a[0];
                    int want = a.length > 1 ? (Integer) a[1] : dst.length;
                    byte[] src = bytes();
                    int n2 = Math.min(want, src.length - readPos);
                    if (n2 <= 0) return -1;
                    System.arraycopy(src, readPos, dst, 0, n2);
                    readPos += n2;
                    return n2;
                }
                case "reset":
                    readPos = 0;
                    return null;
                case "setText":
                    text = (String) a[0];
                    return null;
                case "getText":
                    return text;
                case "setString":
                    map.put((String) a[0], a[1]);
                    return null;
                case "getString":
                    return map.get(a[0]) == null ? null : String.valueOf(map.get(a[0]));
                case "clearBody":
                    map.clear();
                    text = null;
                    written.reset();
                    body = null;
                    readPos = 0;
                    return null;
                case "clearProperties":
                    props.clear();
                    return null;
                case "toString":
                    return type.getSimpleName() + props;
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == a[0];
                default:
                    return TestRig.defaultValue(m.getReturnType());
            }
        }
    }

    static BytesMessage bytes() {
        return (BytesMessage) make(BytesMessage.class);
    }

    static TextMessage text() {
        return (TextMessage) make(TextMessage.class);
    }

    static MapMessage map() {
        return (MapMessage) make(MapMessage.class);
    }

    private static Message make(Class<?> type) {
        return (Message) Proxy.newProxyInstance(FakeJms.class.getClassLoader(), new Class<?>[]{type}, new State(type));
    }

    static State state(Message m) {
        return (State) Proxy.getInvocationHandler(m);
    }

    /** A fresh copy to deliver: same properties and body, read position at the start. */
    static Message copy(Message m) {
        State s = state(m);
        Message c = make(s.type);
        State t = state(c);
        t.props.putAll(s.props);
        t.map.putAll(s.map);
        t.text = s.text;
        byte[] b = s.bytes();
        t.written.write(b, 0, b.length);
        return c;
    }

    /**
     * Does {@code m} match a selector of the forms filerepo and the executor use: {@code a='b'} and bare boolean
     * properties, joined by AND (case-insensitive).
     */
    static boolean matches(String selector, Message m) {
        if (selector == null || selector.trim().isEmpty()) return true;
        Map<String, Object> props = state(m).props;
        for (String term : selector.split("(?i)\\s+and\\s+")) {
            String t = term.trim();
            int eq = t.indexOf('=');
            if (eq < 0) {
                if (!Boolean.parseBoolean(String.valueOf(props.get(t)))) return false;
                continue;
            }
            String key = t.substring(0, eq).trim();
            String val = t.substring(eq + 1).trim();
            if (val.startsWith("'") && val.endsWith("'") && val.length() >= 2) val = val.substring(1, val.length() - 1);
            Object have = props.get(key);
            if (have == null || !val.equals(String.valueOf(have))) return false;
        }
        return true;
    }
}
