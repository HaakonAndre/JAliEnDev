package alien.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author costing
 */
public class Context {

	private static Map<Long, Map<String, Object>> context = new ConcurrentHashMap<Long, Map<String, Object>>();
	
	/**
	 * @param key
	 * @param value
	 * @return the previously set value for this key
	 */
	public static Object setThreadContext(final String key, final Object value){
		Map<String, Object> m = context.get(Long.valueOf(Thread.currentThread().getId()));
		
		if (m==null){
			// the map will be accessed from within the same thread, so there can be no conflict here
			m = new HashMap<String, Object>();
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
