/**
 * 
 */
package alien.servlets;

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import lazyj.ExtendedServlet;
import lazyj.LRUMap;
import lia.util.StringFactory;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author costing
 * @since Apr 28, 2011
 */
public class TextCache extends ExtendedServlet {
	private static final long serialVersionUID = 6024682549531639348L;
	
	private static final class CacheValue {
		public final String value;
		
		public final long expires;
		
		public CacheValue(final String value, final long expires){
			this.value = value;
			this.expires = expires;
		}
	}

	private static final class CleanupThread extends Thread{
		public CleanupThread() {
			setName("alien.servlets.ThreadCache.CleanupThread");
			setDaemon(true);
		}
		
		@Override
		public void run() {
			while (true){
				try{
					Thread.sleep(1000*60);
					
					final long now = System.currentTimeMillis();
					
					for (final Map<String, CacheValue> cache: namespaces.values()){
						synchronized (cache){
							final Iterator<Map.Entry<String, CacheValue>> it = cache.entrySet().iterator();
							
							while (it.hasNext()){
								final Map.Entry<String, CacheValue> entry = it.next();
								
								if (entry.getValue().expires < now)
									it.remove();
							}
						}
					}
				}
				catch (Throwable t){
					// ignore
				}
			}
		}
	}
	
	/**
	 * Goes through the entries and removes the expired ones
	 */
	static final CleanupThread cleanup;
	
	static{
		cleanup = new CleanupThread();
		cleanup.start();
	}
	
	/**
	 * Big cache structure
	 */
	final static Map<String, Map<String, CacheValue>> namespaces = new ConcurrentHashMap<String, Map<String, CacheValue>>();
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(TextCache.class.getCanonicalName());
	
	private static final Map<String, CacheValue> getNamespace(final String name){
		Map<String, CacheValue> ret = namespaces.get(name);
		
		if (ret!=null)
			return ret;
		
		int size;
		
		try{
			try{
				size = Integer.parseInt(System.getProperty("alien.servlets.TextCache.size_"+name));
			}
			catch (Throwable t1){
				size = Integer.parseInt(System.getProperty("alien.servlets.TextCache.size"));
			}
		}
		catch (Throwable t){
			size = 50000;
		}
		
		ret = new LRUMap<String, CacheValue>(size);
		
		namespaces.put(name, ret);
		
		return ret;
	}
	
	private static long lastMonitoringSent = System.currentTimeMillis();
	
	private static final long MONITORING_INTERVAL = 1000*30;
	
	private static final Object monitoringLock = new Object();
	
	private static void sendMonitoringData(){
		synchronized (monitoringLock){
			final long lNow = System.currentTimeMillis();
		
			if (lNow - lastMonitoringSent < MONITORING_INTERVAL){
				return;
			}
			
			lastMonitoringSent = lNow;
		}
		
		final Vector<String> parameters = new Vector<String>();
		final Vector<Object> values = new Vector<Object>();
			
		for (final Map.Entry<String, Map<String, CacheValue>> entry: namespaces.entrySet()){
			parameters.add(entry.getKey()+"_size");
			values.add(Integer.valueOf(entry.getValue().size()));
		}
			
		monitor.sendParameters(parameters, values);
	}
	
	/* (non-Javadoc)
	 * @see lazyj.ExtendedServlet#execGet()
	 */
	@Override
	public void execGet() {
		final long start = System.currentTimeMillis();
		
		execRealGet();
		
		final long duration = System.currentTimeMillis() - start;
		
		monitor.addMeasurement("ms_to_answer", duration);
	}
	
	private final void execRealGet(){
		final String ns = gets("ns", "default");
		
		final String key = gets("key");
		
		response.setContentType("text/plain");
		
		if (key.length()==0){
			if (gets("clean", null)!=null){
				for (final Map.Entry<String, Map<String, CacheValue>> entry: namespaces.entrySet()){
					final Map<String, CacheValue> cache = entry.getValue();
					
					synchronized(cache){
						cache.clear();
					}
				}
			}
			else{
				for (final Map.Entry<String, Map<String, CacheValue>> entry: namespaces.entrySet()){
					pwOut.println(entry.getKey()+" : "+entry.getValue().size());
				}				
			}
			pwOut.flush();
			
			return;
		}
		
		final Map<String, CacheValue> cache = getNamespace(ns);
		
		String value = gets("value", null);
		
		if (value!=null){
			// a SET operation
			
			if (monitor != null)
				monitor.incrementCounter("SET_"+ns);
			
			if (value.indexOf("'eof'")>=0){
				value = StringFactory.get(value);
			}
			
			synchronized(cache){
				cache.put(key, new CacheValue(value, System.currentTimeMillis() + getl("timeout", 60*60)*1000));
			}
			
			return;
		}
		
		CacheValue existing;
		
		synchronized (cache){
			existing = cache.get(key);
		}
		
		if (existing==null){
			if (monitor != null)
				monitor.incrementCounter("NULL_"+ns);
			
			pwOut.println("ERR: null");
			pwOut.flush();
			return;
		}
		
		if (existing.expires < System.currentTimeMillis()){
			if (monitor != null)
				monitor.incrementCounter("EXPIRED_"+ns);
			
			pwOut.println("ERR: expired");
			pwOut.flush();
			return;			
		}
				
		if (monitor != null)
			monitor.incrementCounter("HIT_"+ns);
		
		pwOut.println(existing.value);
		pwOut.flush();
		
		sendMonitoringData();
	}

}
