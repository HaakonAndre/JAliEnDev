/**
 * 
 */
package alien.servlets;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lazyj.ExtendedServlet;
import lazyj.cache.ExpirationCache;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author costing
 * @since Apr 28, 2011
 */
public class TextCache extends ExtendedServlet {
	private static final long serialVersionUID = 6024682549531639348L;
	
	private final static ConcurrentHashMap<String, ExpirationCache<String, String>> namespaces = new ConcurrentHashMap<String, ExpirationCache<String, String>>();
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(TextCache.class.getCanonicalName());
	
	private static final ExpirationCache<String, String> getNamespace(final String name){
		ExpirationCache<String, String> ret = namespaces.get(name);
		
		if (ret!=null)
			return ret;
		
		int size;
		
		try{
			size = Integer.parseInt(System.getProperty("alien.servlets.TextCache.size"));
		}
		catch (Throwable t){
			size = 300000;
		}
		
		ret = new ExpirationCache<String, String>(size);
		
		namespaces.put(name, ret);
		
		return ret;
	}
	
	/* (non-Javadoc)
	 * @see lazyj.ExtendedServlet#execGet()
	 */
	@Override
	public void execGet() {
		final String ns = gets("ns", "default");
		
		final String key = gets("key");
		
		response.setContentType("text/plain");
		
		if (key.length()==0){
			for (Map.Entry<String, ExpirationCache<String, String>> entry: namespaces.entrySet()){
				pwOut.println(entry.getKey()+" : "+entry.getValue().size());
			}
			
			pwOut.flush();
			return;
		}
		
		final ExpirationCache<String, String> cache = getNamespace(ns);
		
		String value = gets("value", null);
		
		if (value!=null){
			// a SET operation
			
			if (monitor != null)
				monitor.incrementCounter("SET_"+ns);
			
			cache.overwrite(key, value, getl("timeout", 60*60)*1000);
			return;
		}
		
		value = cache.get(key);
		
		if (value==null){
			if (monitor != null)
				monitor.incrementCounter("NULL_"+ns);
			
			pwOut.println("ERR: null");
			pwOut.flush();
			return;
		}
		
		if (monitor != null)
			monitor.incrementCounter("HIT_"+ns);
		
		pwOut.println(value);
		pwOut.flush();
	}

}
