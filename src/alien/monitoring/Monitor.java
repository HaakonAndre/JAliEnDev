package alien.monitoring;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import lia.Monitor.monitor.Result;
import lia.util.DynamicThreadPoll.SchJobInt;
import alien.config.ConfigUtils;
import apmon.ApMon;

/**
 * @author costing
 */
public class Monitor implements Runnable {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Monitor.class.getCanonicalName());

	private final String component;

	private Collection<SchJobInt> modules;

	ScheduledFuture<?> future = null;

	private ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<String, Monitor.Counter>();

	private final String clusterName;
	private final String nodeName;

	Monitor(final String component){
		this.component = component;
		
		this.modules = new HashSet<SchJobInt>();
		
		final String clusterPrefix = MonitorFactory.getConfigString(component, "cluster_prefix", "ALIEN");
		final String clusterSuffix = MonitorFactory.getConfigString(component, "cluster_suffix", "Nodes");
		
		String cluster = "";
		
		if (clusterPrefix!=null && clusterPrefix.length()>0)
			cluster = clusterPrefix+"_";
		
		cluster += component;
		
		if (clusterSuffix!=null && clusterSuffix.length()>0)
			cluster += "_"+clusterSuffix;
		
		clusterName = MonitorFactory.getConfigString(component, "cluster_name", cluster);
		
		String hostName = "unknown";
		
		try{
			hostName = java.net.InetAddress.getLocalHost().getCanonicalHostName();
		}
		catch (UnknownHostException uhe){
			logger.log(Level.SEVERE, "Cannot get localhost name!", uhe);
		}
		
		nodeName = MonitorFactory.getConfigString(component, "node_name", hostName);
	}

	void addModule(final SchJobInt module) {
		if (module != null)
			modules.add(module);
	}

	/**
	 * Access counters
	 * 
	 * @author costing
	 */
	public static final class Counter {
		private final AtomicLong counter = new AtomicLong(0);

		private long oldValue = 0;

		/**
		 * Default constructor
		 */
		Counter() {
			// default constructor
		}

		/**
		 * Increment the counter
		 * 
		 * @return the incremented value
		 */
		public long increment() {
			long value = counter.incrementAndGet();

			if (value == Long.MAX_VALUE) {
				// reset counters when overflowing
				value = 1;
				counter.set(value);
				oldValue = 0;
			}

			return value;
		}

		/**
		 * @return current absolute value of the counter
		 */
		public long longValue() {
			return counter.get();
		}

		/**
		 * Get the increment rate in changes/second, since the last call
		 * 
		 * @param timeInterval interval in seconds
		 * @return changes/second
		 */
		double getRate(final double timeInterval) {
			if (timeInterval <= 0)
				return Double.NaN;

			final long value = longValue();

			double ret = Double.NaN;

			if (value >= oldValue) {
				ret = (value - oldValue) / timeInterval;
			}

			oldValue = value;

			return ret;
		}
	}

	/**
	 * Increment an access counter
	 * 
	 * @param counterKey
	 * @return the new absolute value of the counter
	 */
	public long incrementCounter(final String counterKey) {
		Counter c = counters.get(counterKey);

		if (c == null) {
			c = new Counter();

			counters.put(counterKey, c);
		}

		return c.increment();
	}

	private long lLastTimestamp = System.currentTimeMillis();

	@Override
	public void run() {
		final ApMon apmon = MonitorFactory.getApMonSender();

		if (apmon == null)
			return;

		final List<Object> values = new ArrayList<Object>();

		for (final SchJobInt module : modules) {
			final Object o;

			try {
				o = module.doProcess();
			}
			catch (Throwable t) {
				logger.log(Level.WARNING, "Exception running module " + module + " for component " + component, t);

				continue;
			}

			if (o == null)
				continue;

			if (o instanceof Collection<?>) {
				values.addAll((Collection<?>) o);
			}
			else
				values.add(o);
		}

		final Result rates = new Result();

		final long lNow = System.currentTimeMillis();

		final double diff = (lNow - lLastTimestamp) / 1000d;

		lLastTimestamp = lNow;

		for (final Map.Entry<String, Counter> me : counters.entrySet()) {
			final double dRate = me.getValue().getRate(diff);

			if (!Double.isNaN(dRate))
				rates.addSet(me.getKey(), dRate);
		}

		if (rates.param != null && rates.param.length > 0)
			values.add(rates);

		if (values.size() > 0)
			sendResults(values);
	}

	/**
	 * Send a bunch of results
	 * 
	 * @param values
	 */
	public void sendResults(final Collection<Object> values){
		// TODO actual sending
	}
}
