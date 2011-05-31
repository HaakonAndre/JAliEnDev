package alien.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author costing
 */
public class Context {

	/**
	 * Thread-tied contexts
	 */
	static Map<Long, Map<String, Object>> context = new ConcurrentHashMap<Long, Map<String, Object>>();
	
	static {
		final Thread cleanupThread = new Thread("alien.config.Context.cleanup"){
			@Override
			public void run() {
				while (true){
					try{
						Thread.sleep(1000*60);
						
						final Set<Long> threadIDs = new HashSet<Long>();
						
						for (final Thread t: Thread.getAllStackTraces().keySet()){
							threadIDs.add(Long.valueOf(t.getId()));
						}
						
						final Iterator<Map.Entry<Long, Map<String, Object>>> it = context.entrySet().iterator();
						
						while (it.hasNext()){
							final Map.Entry<Long, Map<String, Object>> me = it.next();
							
							if (!threadIDs.contains(me.getKey()))
								it.remove();
						}
					}
					catch (final Throwable t){
						// ignore
					}
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
	public static Object setThreadContext(final String key, final Object value){
		final Long threadID = Long.valueOf(Thread.currentThread().getId());
		
		Map<String, Object> m = context.get(threadID);
		
		if (m==null){
			// the map will be accessed from within the same thread, so there can be no conflict here
			m = new HashMap<String, Object>();
			context.put(threadID, m);
		}
		
		return m.put(key, value);
	}
	
	/**
	 * @param key
	 * @return the value associated with this key for the current thread
	 */
	public static Object getTheadContext(final String key){
		final Map<String, Object> m = context.get(Long.valueOf(Thread.currentThread().getId()));
		
		if (m==null)
			return null;
		
		return m.get(key);
	}
	
}
