package alien.api;

import java.lang.ref.WeakReference;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import lazyj.cache.ExpirationCache;

/**
 * @author costing
 * @since 2011-03-04
 */
public class Dispatcher {

	private static final boolean useParallelConnections = false;

	private static final ExpirationCache<String, WeakReference<Request>> cache = new ExpirationCache<>(10240);

	static transient final Monitor monitor = MonitorFactory.getMonitor(Dispatcher.class.getCanonicalName());

	static {
		monitor.addMonitoring("object_cache_status", (names, values) -> {
			names.add("object_cache_size");
			values.add(Double.valueOf(cache.size()));
		});
	}

	/**
	 * @param r
	 *            request to execute
	 * @return the processed request
	 * @throws ServerException
	 *             exception thrown by the processing
	 */
	public static <T extends Request> T execute(final T r) throws ServerException {
		return execute(r, false);
	}

	/**
	 * @param r
	 *            request to execute
	 * @param forceRemote
	 *            request to force remote execution
	 * @return the processed request
	 * @throws ServerException
	 *             exception thrown by the processing
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Request> T execute(final T r, final boolean forceRemote) throws ServerException {
		final boolean isCacheable = r instanceof Cacheable;

		final String key;

		if (isCacheable)
			key = r.getClass().getCanonicalName() + "#" + ((Cacheable) r).getKey();
		else
			key = null;

		if (r instanceof Cacheable) {
			final WeakReference<Request> cachedObject = cache.get(key);

			final Object cachedValue;

			if (cachedObject != null && (cachedValue = cachedObject.get()) != null) {
				monitor.incrementCacheHits("object_cache");
				return (T) cachedValue;
			}
			monitor.incrementCacheMisses("object_cache");
		}
		else
			monitor.incrementCounter("non_cacheable");

		final T ret;

		final Timing timing = new Timing();

		if (ConfigUtils.isCentralService() && !forceRemote) {
			r.authorizeUserAndRole();
			r.run();
			ret = r;

			monitor.addMeasurement("executed_requests", timing);
		}
		else {
			ret = dispatchRequest(r);

			monitor.addMeasurement("forwarded_requests", timing);
		}

		if (isCacheable && ret != null)
			cache.put(key, new WeakReference<Request>(ret), ((Cacheable) ret).getTimeout());

		return ret;
	}

	private static <T extends Request> T dispatchRequest(final T r) throws ServerException {

		// return DispatchSSLClient.dispatchRequest(r);
		return useParallelConnections ? DispatchSSLMTClient.dispatchRequest(r) : DispatchSSLClient.dispatchRequest(r);
	}

}
