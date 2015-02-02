/**
 * 
 */
package alien.servlets;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import lazyj.ExtendedServlet;
import lazyj.Format;
import lazyj.LRUMap;
import lazyj.Utils;
import lazyj.cache.ExpirationCache;
import lia.Monitor.monitor.ShutdownReceiver;
import lia.util.ShutdownManager;
import lia.util.StringFactory;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author costing
 * @since Apr 28, 2011
 */
public class TextCache extends ExtendedServlet {
	private static final long serialVersionUID = 6024682549531639348L;

	private static final ExpirationCache<String, Integer> defaultNamespaceExpiration = new ExpirationCache<>();

	private static int getDefaultExpiration(final String namespace) {
		final Integer i = defaultNamespaceExpiration.get(namespace);

		if (i != null)
			return i.intValue();

		int nsDefault = 60 * 60;

		try {
			try {
				nsDefault = Integer.parseInt(System.getProperty("alien.servlets.TextCache.ttl_" + namespace));
			} catch (final Throwable t1) {
				nsDefault = Integer.parseInt(System.getProperty("alien.servlets.TextCache.ttl"));
			}
		} catch (final Throwable t) {
			// ignore
		}

		defaultNamespaceExpiration.put(namespace, Integer.valueOf(nsDefault), 60 * 5 * 1000);

		return nsDefault;
	}

	private static final class CacheValue {
		public final String value;

		public final long expires;

		public final AtomicInteger accesses = new AtomicInteger(1);

		public CacheValue(final String value, final long expires) {
			this.value = value;
			this.expires = expires;
		}
	}

	private static final class EntryComparator implements Comparator<Map.Entry<String, CacheValue>>, Serializable {
		private static final long serialVersionUID = -6092398826822045152L;

		public EntryComparator() {
			// nothing
		}

		@Override
		public int compare(final Entry<String, CacheValue> o1, final Entry<String, CacheValue> o2) {
			final int diff = o2.getValue().accesses.intValue() - o1.getValue().accesses.intValue();

			if (diff != 0)
				return diff;

			return o2.getKey().compareTo(o1.getKey());
		}

	}

	private static final EntryComparator entryComparator = new EntryComparator();

	private static final class CleanupThread extends Thread {
		public CleanupThread() {
			setName("alien.servlets.ThreadCache.CleanupThread");
			setDaemon(true);
		}

		@Override
		public void run() {
			while (true)
				try {
					Thread.sleep(1000 * 30);

					final long now = System.currentTimeMillis();

					final Vector<String> parameters = new Vector<>();
					final Vector<Object> values = new Vector<>();

					for (final Map.Entry<String, Map<String, CacheValue>> nsEntry : namespaces.entrySet()) {
						final String ns = nsEntry.getKey();
						final Map<String, CacheValue> cache = nsEntry.getValue();

						long soonestToExpire = 0;
						long latestToExpire = now;

						synchronized (cache) {
							final Iterator<Map.Entry<String, CacheValue>> it = cache.entrySet().iterator();

							while (it.hasNext()) {
								final Map.Entry<String, CacheValue> entry = it.next();

								final long expires = entry.getValue().expires;

								if (expires < now) {
									notifyEntryRemoved(ns, entry.getKey(), entry.getValue());

									it.remove();
								} else {
									if (soonestToExpire == 0 || expires < soonestToExpire)
										soonestToExpire = expires;

									if (expires > latestToExpire)
										latestToExpire = expires;
								}
							}

							parameters.add(ns + "_size");
							values.add(Integer.valueOf(cache.size()));
						}

						if (soonestToExpire > 0) {
							parameters.add(ns + "_hours");
							values.add(Double.valueOf((latestToExpire - soonestToExpire) / (3600000d)));
						}
					}

					monitor.sendParameters(parameters, values);
				} catch (final Throwable t) {
					// ignore
				}
		}
	}

	/**
	 * Goes through the entries and removes the expired ones
	 */
	static final CleanupThread cleanup;

	static {
		cleanup = new CleanupThread();
		cleanup.start();

		ShutdownManager.getInstance().addModule(new ShutdownReceiver() {

			@Override
			public void Shutdown() {
				for (final Map.Entry<String, Map<String, CacheValue>> entry : namespaces.entrySet()) {
					final Map<String, CacheValue> cache = entry.getValue();
					synchronized (cache) {
						for (final Map.Entry<String, CacheValue> entryToDelete : cache.entrySet())
							notifyEntryRemoved(entry.getKey(), entryToDelete.getKey(), entryToDelete.getValue());
					}
				}
			}
		});
	}

	/**
	 * Big cache structure
	 */
	final static Map<String, Map<String, CacheValue>> namespaces = new ConcurrentHashMap<>();

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(TextCache.class.getCanonicalName());

	private static PrintWriter requestLogger = null;

	private static int logCounter = 0;

	/**
	 * Call this one entry is removed to log the number of hits
	 * 
	 * @param namespace
	 * @param key
	 * @param value
	 */
	static synchronized void notifyEntryRemoved(final String namespace, final String key, final CacheValue value) {
		if (requestLogger == null)
			try {
				requestLogger = new PrintWriter(new FileWriter("cache.log", true));
			} catch (final IOException e) {
				System.err.println("Could not write to cache.log");
				return;
			}

		requestLogger.println(System.currentTimeMillis() + " " + value.accesses + " " + namespace + " " + key);

		if (requestLogger.checkError())
			requestLogger = null;
		else if (++logCounter > 1000) {
			logCounter = 0;

			final File f = new File("cache.log");

			if (!f.exists())
				requestLogger = null;
		}
	}

	/**
	 * @author costing
	 * 
	 */
	public static final class NotifyLRUMap extends LRUMap<String, CacheValue> {
		private static final long serialVersionUID = -9117776082771411054L;

		private final String namespace;

		/**
		 * @param iCacheSize
		 * @param namespace
		 */
		public NotifyLRUMap(final int iCacheSize, final String namespace) {
			super(iCacheSize);

			this.namespace = namespace;
		}

		@Override
		protected boolean removeEldestEntry(final java.util.Map.Entry<String, CacheValue> eldest) {
			final boolean ret = super.removeEldestEntry(eldest);

			if (ret)
				notifyEntryRemoved(namespace, eldest.getKey(), eldest.getValue());

			return ret;
		}

	}

	private static final Map<String, CacheValue> getNamespace(final String name) {
		Map<String, CacheValue> ret = namespaces.get(name);

		if (ret != null)
			return ret;

		int size;

		try {
			try {
				size = Integer.parseInt(System.getProperty("alien.servlets.TextCache.size_" + name));
			} catch (final Throwable t1) {
				size = Integer.parseInt(System.getProperty("alien.servlets.TextCache.size"));
			}
		} catch (final Throwable t) {
			size = 50000;
		}

		ret = new NotifyLRUMap(size, name);

		namespaces.put(name, ret);

		return ret;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see lazyj.ExtendedServlet#execGet()
	 */
	@Override
	public void execGet() {
		final long start = System.nanoTime();

		execRealGet();

		final long duration = System.nanoTime() - start;

		monitor.addMeasurement("ms_to_answer", duration / 1000000d);
	}

	private final void execRealGet() {
		final String ns = gets("ns", "default");

		final String key = gets("key");

		response.setContentType("text/plain");

		if (key.length() == 0) {
			if (gets("clean", null) != null)
				for (final Map.Entry<String, Map<String, CacheValue>> entry : namespaces.entrySet()) {
					final Map<String, CacheValue> cache = entry.getValue();

					synchronized (cache) {
						for (final Map.Entry<String, CacheValue> entryToDelete : cache.entrySet())
							notifyEntryRemoved(entry.getKey(), entryToDelete.getKey(), entryToDelete.getValue());

						cache.clear();
					}
				}
			else if (gets("ns").length() == 0) {
				for (final Map.Entry<String, Map<String, CacheValue>> entry : namespaces.entrySet()) {
					final Map<String, CacheValue> cache = entry.getValue();

					int min = -1;
					int max = 0;
					long total = 0;
					double avg;

					long hits = 0;

					synchronized (cache) {
						for (final CacheValue c : cache.values()) {
							final int size = c.value.length();

							min = (min < 0 || size < min) ? size : min;
							max = Math.max(max, size);
							total += size;

							hits += c.accesses.intValue();
						}

						avg = (double) total / cache.size();
					}

					if (min < 0)
						pwOut.println(entry.getKey() + " : empty");
					else
						pwOut.println(entry.getKey() + " : " + cache.size() + " (min: " + min + ", avg: " + Format.point(avg) + ", max: " + max + ", total: " + Format.size(total) + ") : " + hits
								+ " hits");
				}

				final Runtime r = Runtime.getRuntime();

				pwOut.println("\nJava memory stats: " + Format.size(r.totalMemory()) + " total memory, " + Format.size(r.maxMemory()) + " max memory, " + Format.size(r.freeMemory()) + " free");
				pwOut.println("Java version: " + System.getProperty("java.version"));
				pwOut.println("Uptime: " + Format.toInterval(ManagementFactory.getRuntimeMXBean().getUptime()));
			} else {
				final Map<String, CacheValue> cache = namespaces.get(ns);

				if (cache == null || cache.size() == 0)
					pwOut.println("Namespace is empty");
				else {
					int min = -1;
					int max = 0;
					long total = 0;
					double avg;
					int hits = 0;

					final boolean values = gets("values").length() > 0;

					synchronized (cache) {
						final ArrayList<Map.Entry<String, CacheValue>> entries = new ArrayList<>(cache.entrySet());

						Collections.sort(entries, entryComparator);

						for (final Map.Entry<String, CacheValue> me : entries) {
							final CacheValue cv = me.getValue();

							final int size = cv.value.length();

							min = (min < 0 || size < min) ? size : min;
							max = Math.max(max, size);
							total += size;

							hits += cv.accesses.intValue();

							pwOut.println(me.getKey() + " : size " + size + ", " + cv.accesses + " hits" + (values ? " : " + cv.value : ""));
						}

						avg = (double) total / cache.size();
					}

					pwOut.println("\n\n----------------\n\n" + cache.size() + " entries (min: " + min + ", avg: " + Format.point(avg) + ", max: " + max + ", total: " + Format.size(total) + ") : "
							+ hits + " hits");
				}
			}
			pwOut.flush();

			return;
		}

		final Map<String, CacheValue> cache = getNamespace(ns);

		String value = gets("value", null);

		if (value != null) {
			// a SET operation

			CacheValue old;

			if (getb("ifnull", false) == true) {
				synchronized (cache) {
					old = cache.get(key);
				}

				if (old != null && old.expires >= System.currentTimeMillis()) {
					if (monitor != null)
						monitor.incrementCounter("SET_WAITING_" + ns);

					return;
				}
			}

			if (monitor != null)
				monitor.incrementCounter("SET_" + ns);

			if (value.indexOf("eof") >= 0) {
				value = StringFactory.get(value);

				if (monitor != null)
					monitor.incrementCounter("SET_EOF_" + ns);
			}

			synchronized (cache) {
				old = cache.put(key, new CacheValue(value, System.currentTimeMillis() + getl("timeout", getDefaultExpiration(ns)) * 1000));
			}

			if (old != null)
				notifyEntryRemoved(ns, key, old);

			return;
		}

		if (getb("clear", false)) {
			int removed = 0;

			for (final String keyValue : getValues("key")) {
				String sLargestPart = "";

				final String[] parts = keyValue.split("\\.(\\+|\\*)");

				for (final String part : parts)
					if (part.length() > sLargestPart.length())
						sLargestPart = part;

				if (sLargestPart.equals(keyValue)) {
					CacheValue old;

					synchronized (cache) {
						old = cache.remove(keyValue);
					}

					if (old != null) {
						notifyEntryRemoved(ns, keyValue, old);
						removed++;
					}

					continue;
				}

				final Pattern p;

				try {
					p = Pattern.compile("^" + keyValue + "$");
				} catch (final PatternSyntaxException e) {
					pwOut.println("ERR: invalid pattern syntax: " + keyValue);
					pwOut.flush();
					return;
				}

				synchronized (cache) {
					final Iterator<Map.Entry<String, CacheValue>> it = cache.entrySet().iterator();

					Matcher m = null;

					while (it.hasNext()) {
						final Map.Entry<String, CacheValue> entry = it.next();

						final String itKey = entry.getKey();

						if (sLargestPart.length() > 0 && itKey.indexOf(sLargestPart) < 0)
							continue;

						if (m == null)
							m = p.matcher(itKey);
						else
							m.reset(itKey);

						if (m.matches()) {
							notifyEntryRemoved(ns, entry.getKey(), entry.getValue());
							it.remove();
							removed++;
						}
					}
				}

				if (monitor != null)
					monitor.incrementCounter("CLEARPATTERN_" + ns);
			}

			pwOut.println("OK: removed " + removed + " values from ns '" + ns + "' matching " + Arrays.toString(getValues("key")));
			pwOut.flush();

			return;
		}

		CacheValue existing;

		synchronized (cache) {
			existing = cache.get(key);
		}

		if (existing == null) {
			if (monitor != null)
				monitor.incrementCounter("NULL_" + ns);

			pwOut.println("ERR: null");
			pwOut.flush();
			return;
		}

		if (existing.expires < System.currentTimeMillis()) {
			if (monitor != null)
				monitor.incrementCounter("EXPIRED_" + ns);

			pwOut.println("ERR: expired");
			pwOut.flush();
			return;
		}

		existing.accesses.incrementAndGet();

		if (monitor != null)
			monitor.incrementCounter("HIT_" + ns);

		if (existing.value.indexOf("eof") >= 0)
			if (monitor != null)
				monitor.incrementCounter("HIT_EOF_" + ns);

		pwOut.println(existing.value);
		pwOut.flush();
	}

	/**
	 * @param baseURL
	 *            URL to TextCache
	 * @param ns
	 *            namespace
	 * @param pattern
	 *            key pattern to remove
	 * @return the outcome of the query as indicated by the server
	 * @throws IOException
	 */
	public static String invalidateCacheEntry(final String baseURL, final String ns, final String pattern) throws IOException {
		return Utils.download(baseURL + "?ns=" + Format.encode(ns) + "&key=" + Format.encode(pattern) + "&clear=true", null);
	}

}
