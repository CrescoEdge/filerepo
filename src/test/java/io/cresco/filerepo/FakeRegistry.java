package io.cresco.filerepo;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Just enough of an OSGi service registry for a real {@code org.osgi.util.tracker.ServiceTracker}: per-bundle contexts,
 * filtered service listeners (the framework's own filter implementation), registration events delivered on the
 * registering thread, and services got through a factory that sees the asking bundle, as a ServiceFactory does.
 */
final class FakeRegistry {

    private final AtomicLong ids = new AtomicLong(1);
    private final List<Listening> listeners = new CopyOnWriteArrayList<>();
    private final Map<ServiceReference<?>, Function<Bundle, Object>> services = new ConcurrentHashMap<>();
    /** The symbolic names of the bundles a factory was asked by, in order. */
    final List<String> askedBy = Collections.synchronizedList(new ArrayList<>());

    private static final class Listening {
        final ServiceListener listener;
        final Filter filter;

        Listening(ServiceListener listener, Filter filter) {
            this.listener = listener;
            this.filter = filter;
        }
    }

    static Bundle bundle(String symbolicName) {
        return (Bundle) Proxy.newProxyInstance(Bundle.class.getClassLoader(), new Class<?>[]{Bundle.class}, (p, m, a) -> {
            switch (m.getName()) {
                case "getSymbolicName": return symbolicName;
                case "getBundleId": return (long) Math.abs(String.valueOf(symbolicName).hashCode());
                case "toString": return "bundle " + symbolicName;
                case "hashCode": return System.identityHashCode(p);
                case "equals": return p == a[0];
                default: return null;
            }
        });
    }

    /** The context the bundle named {@code symbolicName} sees. */
    BundleContext contextOf(String symbolicName) {
        Bundle me = bundle(symbolicName);
        return (BundleContext) Proxy.newProxyInstance(BundleContext.class.getClassLoader(), new Class<?>[]{BundleContext.class}, (p, m, a) -> {
            switch (m.getName()) {
                case "createFilter":
                    return FrameworkUtil.createFilter((String) a[0]);
                case "addServiceListener":
                    listeners.add(new Listening((ServiceListener) a[0],
                            a.length > 1 && a[1] != null ? FrameworkUtil.createFilter((String) a[1]) : null));
                    return null;
                case "removeServiceListener":
                    listeners.removeIf(l -> l.listener == a[0]);
                    return null;
                case "getServiceReferences":
                case "getAllServiceReferences":
                    return references((String) a[0], (String) a[1]);
                case "getService": {
                    Function<Bundle, Object> factory = services.get((ServiceReference<?>) a[0]);
                    if (factory == null) return null;
                    askedBy.add(symbolicName);
                    return factory.apply(me);
                }
                case "ungetService":
                    return true;
                case "getBundle":
                    return me;
                case "toString":
                    return "context of " + symbolicName;
                case "hashCode":
                    return System.identityHashCode(p);
                case "equals":
                    return p == a[0];
                default:
                    throw new UnsupportedOperationException("FakeRegistry: " + m.getName());
            }
        });
    }

    /** Register a Function service with {@code props}; {@code factory} decides, per asking bundle, what it gets (null: nothing). */
    ServiceReference<?> register(Map<String, Object> props, Function<Bundle, Object> factory) {
        return register(new String[]{Function.class.getName()}, props, factory);
    }

    /** Register a service under {@code classes}, delivering REGISTERED to every listener whose filter matches. */
    ServiceReference<?> register(String[] classes, Map<String, Object> props, Function<Bundle, Object> factory) {
        Hashtable<String, Object> all = new Hashtable<>(props);
        all.put(Constants.OBJECTCLASS, classes);
        all.put(Constants.SERVICE_ID, ids.getAndIncrement());
        Bundle owner = bundle("provider");
        ServiceReference<?> ref = (ServiceReference<?>) Proxy.newProxyInstance(ServiceReference.class.getClassLoader(),
                new Class<?>[]{ServiceReference.class}, (p, m, a) -> {
                    switch (m.getName()) {
                        case "getProperty": return all.get((String) a[0]);
                        case "getPropertyKeys": return all.keySet().toArray(new String[0]);
                        case "getProperties": return (Dictionary<String, Object>) new Hashtable<>(all);
                        case "getBundle": return services.containsKey((ServiceReference<?>) p) ? owner : null;
                        case "isAssignableTo": return true;
                        case "getUsingBundles": return null;
                        case "compareTo": return Long.compare((Long) ((ServiceReference<?>) a[0]).getProperty(Constants.SERVICE_ID), (Long) all.get(Constants.SERVICE_ID));
                        case "toString": return "service " + all.get(Constants.SERVICE_ID) + " " + props;
                        case "hashCode": return System.identityHashCode(p);
                        case "equals": return p == a[0];
                        default: throw new UnsupportedOperationException("FakeRegistry ref: " + m.getName());
                    }
                });
        services.put(ref, factory);
        fire(new ServiceEvent(ServiceEvent.REGISTERED, ref));
        return ref;
    }

    /** Unregister, delivering UNREGISTERING to every listener whose filter matches. */
    void unregister(ServiceReference<?> ref) {
        if (!services.containsKey(ref)) return;
        fire(new ServiceEvent(ServiceEvent.UNREGISTERING, ref));
        services.remove(ref);
    }

    private void fire(ServiceEvent e) {
        for (Listening l : listeners) {
            if (l.filter == null || l.filter.match(e.getServiceReference())) l.listener.serviceChanged(e);
        }
    }

    private ServiceReference<?>[] references(String clazz, String filter) throws Exception {
        Filter f = filter == null ? null : FrameworkUtil.createFilter(filter);
        List<ServiceReference<?>> out = new ArrayList<>();
        for (ServiceReference<?> ref : services.keySet()) {
            if (clazz != null) {
                boolean named = false;
                for (String c : (String[]) ref.getProperty(Constants.OBJECTCLASS)) named |= c.equals(clazz);
                if (!named) continue;
            }
            if (f == null || f.match(ref)) out.add(ref);
        }
        return out.isEmpty() ? null : out.toArray(new ServiceReference<?>[0]);
    }
}
