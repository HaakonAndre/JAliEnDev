package alien.monitoring;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.ExtProperties;
import lazyj.Utils;
import lia.Monitor.modules.monProcLoad;
import lia.Monitor.modules.monProcStat;
import alien.config.ConfigUtils;
import apmon.ApMon;
import apmon.ApMonException;

/**
 * @author costing
 */
public final class MonitorFactory {

	/**
	 * For giving incremental thread IDs
	 */
	static final AtomicInteger aiFactoryIndex = new AtomicInteger(0);
	
	private static final ThreadFactory threadFactory = new ThreadFactory() {
		
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r);
			
			t.setName("alien.monitor.MonitorFactory - "+aiFactoryIndex.incrementAndGet());
			t.setDaemon(true);
			
			return t;
		}
	};
	
	private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, threadFactory);
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(MonitorFactory.class.getCanonicalName());
	
	private MonitorFactory(){
		// disable this constructor, only static methods
	}
	
	private static final Map<String, Monitor> monitors = new HashMap<String, Monitor>();
	
	/**
	 * Get the monitor for this component
	 * 
	 * @param component
	 * @return the monitor
	 */
	public static Monitor getMonitor(final String component){
		Monitor m;
		
		synchronized (monitors){
			m = monitors.get(component);
		
			if (m==null && getConfigBoolean(component, "enabled", true)){
				m = new Monitor(component);
			
				final int interval = getConfigInt(component, "period", 60);
				
				final ScheduledFuture<?> future = executor.scheduleAtFixedRate(m, interval/2, interval, TimeUnit.SECONDS);
				
				m.future = future;
				
				monitors.put(component, m);
			}
		}
		
		return m;
	}
	
	private static Monitor systemMonitor = null;
	
	/**
	 * Enable periodic sending of background host monitoring 
	 */
	public static synchronized void enableSystemMonitoring(){
		if (systemMonitor!=null)
			return;
		
		systemMonitor = getMonitor("System");
		
		try {
			systemMonitor.addModule(new monProcStat());
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "Cannot instantiate monProcStat", e);
		}
		
		try {
			systemMonitor.addModule(new monProcLoad());
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "Cannot instantiate monProcLoad", e);
		}
	}
	
	private static Monitor selfMonitor = null;
	
	/**
	 * Enable periodic sending of internal parameters
	 */
	public static synchronized void enableSelfMonitoring(){
		if (selfMonitor!=null)
			return;
		
		selfMonitor = getMonitor("Self");
		
		// TODO implement the module for self JVM monitoring
		// @see myMon
	}
	
	private static ApMon apmonInstance = null;
	private static Object apmonLock = new Object();
	
	private static Vector<String> getApMonDestinations(){
		final ExtProperties p = getConfig();
		
		if (p==null){
			Vector<String> v = new Vector<String>(1);
			v.add("localhost");
			return v;
		}
		
		return p.toVector("destinations");
	}

	/**
	 * @return monitoring configuration
	 */
	static ExtProperties getConfig(){
		return ConfigUtils.getConfiguration("monitoring");
	}
	
	/**
	 * @param component
	 * @param key
	 * @param defaultValue
	 * @return the boolean value for this key
	 */
	static boolean getConfigBoolean(final String component, final String key, final boolean defaultValue){
		final String sValue = getConfigString(component, key, null);

		return Utils.stringToBool(sValue, defaultValue);
	}
	
	/**
	 * @param component
	 * @param key
	 * @param defaultValue
	 * @return the double value for this key
	 */
	static double getConfigDouble(final String component, final String key, final double defaultValue){
		final String sValue = getConfigString(component, key, null);
		
		if (sValue==null)
			return defaultValue;
		
		try{
			return Double.parseDouble(sValue);
		}
		catch (NumberFormatException nfe){
			return defaultValue;
		}		
	}
	
	/**
	 * @param component
	 * @param key
	 * @param defaultValue
	 * @return the integer value for this key
	 */
	static int getConfigInt(final String component, final String key, final int defaultValue){
		final String sValue = getConfigString(component, key, null);
		
		if (sValue==null)
			return defaultValue;
		
		try{
			return Integer.parseInt(sValue);
		}
		catch (NumberFormatException nfe){
			return defaultValue;
		}
	}
	
	/**
	 * @param component
	 * @param key
	 * @param defaultValue
	 * @return the string value for this key
	 */
	static String getConfigString(final String component, final String key, final String defaultValue){
		final ExtProperties prop = MonitorFactory.getConfig();
		
		if (prop==null)
			return defaultValue;
		
		String comp = component;
		
		while (comp!=null){
			final String sValue = prop.gets(comp+".monitor."+key, null);
			
			if (sValue!=null)
				return sValue;
			
			if (comp.length()==0)
				break;
			
			final int idx = comp.lastIndexOf('.');
			
			if (idx>=0)
				comp = comp.substring(0, idx);
			else
				comp = "";
		}
		
		return defaultValue;
	}
	
	/**
	 * Get the ApMon sender
	 * 
	 * @return the sender
	 */
	static ApMon getApMonSender(){
		if (apmonInstance!=null)
			return apmonInstance;
		
		synchronized (apmonLock){
			if (apmonInstance==null){
				final Vector<String> destinations = getApMonDestinations();
				
				logger.log(Level.FINE, "ApMon destinations : "+ destinations);
				
				try{
					apmonInstance = new ApMon(destinations);
				}
				catch (IOException ioe){
					logger.log(Level.SEVERE, "Cannot instantiate ApMon because IOException ", ioe);
				}
				catch (ApMonException e) {
					logger.log(Level.SEVERE, "Cannot instantiate ApMon because ApMonException ", e);
				}
			}
		}
		
		return apmonInstance;
	}
}
