package alien.api;

import java.lang.ref.WeakReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.taskQueue.GetMatchJob;
import alien.api.taskQueue.PutJobLog;
import alien.api.taskQueue.SetJobStatus;
import alien.api.token.GetTokenCertificate;
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

	private static final Monitor monitor = MonitorFactory.getMonitor(Dispatcher.class.getCanonicalName());

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(Dispatcher.class.getCanonicalName());

	static {
		monitor.addMonitoring("object_cache_status", (names, values) -> {
			names.add("object_cache_size");
			values.add(Double.valueOf(cache.size()));
		});

		// make sure the Version class is initialized
		ConfigUtils.getVersion();
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

		if (isCacheable) {
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

		try (Timing timing = new Timing()) {
			if (ConfigUtils.isCentralService() && !forceRemote) {
				r.authorizeUserAndRole();

				if (passesFirewallRules(r)) {
					try {
						r.run();
					}
					catch (final Throwable t) {
						throw new ServerException(t.getMessage(), t);
					}
					finally {
						monitor.addMeasurement("executed_requests", timing);
					}
				}
				else {
					monitor.addMeasurement("firewalled_requests", timing);
				}

				ret = r;
			}
			else {
				try {
					ret = dispatchRequest(r);
				}
				finally {
					monitor.addMeasurement("forwarded_requests", timing);
				}
			}
		}

		if (isCacheable && ret != null)
			cache.put(key, new WeakReference<Request>(ret), ((Cacheable) ret).getTimeout());

		return ret;
	}

	/**
	 * Check if the request should be allowed run
	 *
	 * @param r request to check
	 * @return <code>true</code> if it can be run, <code>false</code> if not
	 */
	private static final boolean passesFirewallRules(final Request r) {
		if (r.getEffectiveRequester().isJobAgent() && !(r instanceof GetMatchJob)) {
			// Allowing the JobAgent to change the job status enables it to act on possible JobWrapper terminations/faults
			if (r instanceof SetJobStatus)
				return true;

			// Enables the JobAgent to report its progress/the resources it allocates for the JobWrapper sandbox
			if (r instanceof PutJobLog)
				return true;

			// Allows JobAgents to retrieve job token certificates for the actual job to run with
			if (r instanceof GetTokenCertificate)
				return true;

			// TODO : add above all commands that a JobAgent should run (setting job status, uploading traces)
			logger.log(Level.SEVERE, "A request was firewalled: " + r.getClass().getName() + " as " + r.getEffectiveRequester());

			r.setException(new ServerException("You are not allowed to call " + r.getClass().getName() + " as job agent", null));
			return false;
		}

		if (r.getEffectiveRequester().isJob()) {
			// TODO : firewall all the commands that the job can have access to (whereis, access (read only for anything but the output directory ...))
		}

		return true;
	}

	private static <T extends Request> T dispatchRequest(final T r) throws ServerException {

		// return DispatchSSLClient.dispatchRequest(r);
		return useParallelConnections ? DispatchSSLMTClient.dispatchRequest(r) : DispatchSSLClient.dispatchRequest(r);
	}

}
