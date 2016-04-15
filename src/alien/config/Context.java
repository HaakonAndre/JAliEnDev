package alien.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author costing
 */
public class Context {

	/**
	 * Thread-tied contexts
	 */
	static Map<Thread, Map<String, Object>> context = new ConcurrentHashMap<>();

	static {
		final Thread cleanupThread = new Thread("alien.config.Context.cleanup") {
			@Override
			public void run() {
				while (true)
					try {
						Thread.sleep(1000 * 60);

						context.keySet().retainAll(Thread.getAllStackTraces().keySet());
					} catch (@SuppressWarnings("unused") final Throwable t) {
						// ignore
					}
			}
		};

		cleanupThread.setDaemon(true);
		cleanupThread.start();
	}

	/**
	 * @param key
	 * @param value
	 * @return the previously set value for this key
	 */
	public static Object setThreadContext(final String key, final Object value) {
		Map<String, Object> m = context.get(Thread.currentThread());

		if (m == null) {
			// the map will be accessed from within the same thread, so there
			// can be no conflict here
			m = new HashMap<>();
			context.put(Thread.currentThread(), m);
		}

		return m.put(key, value);
	}

	/**
	 * @param key
	 * @return the value associated with this key for the current thread
	 */
	public static Object getTheadContext(final String key) {
		final Map<String, Object> m = context.get(Thread.currentThread());

		if (m == null)
			return null;

		return m.get(key);
	}

}
