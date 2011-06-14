/**
 * 
 */
package alien.servlets;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import lazyj.ExtendedServlet;
import lazyj.Format;
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
	
	private static final class CacheValue{
		public final String value;
		
		public final long expires;
		
		public int accesses = 0;
		
		public CacheValue(final String value, final long expires){
			this.value = value;
			this.expires = expires;
		}
	}
	
	private static final class EntryComparator implements Comparator <Map.Entry<String, CacheValue>>{

		@Override
		public int compare(final Entry<String, CacheValue> o1, final Entry<String, CacheValue> o2) {
			final int diff = o2.getValue().accesses - o1.getValue().accesses;
			
			if (diff!=0)
				return diff;
			
			return o2.getKey().compareTo(o1.getKey());
		}
		
	}
	
	private static final EntryComparator entryComparator = new EntryComparator();

	private static final class CleanupThread extends Thread{
		public CleanupThread() {
			setName("alien.servlets.ThreadCache.CleanupThread");
			setDaemon(true);
		}
		
		@Override
		public void run() {
			while (true){
				try{
					Thread.sleep(1000*30);
					
					final long now = System.currentTimeMillis();
										
					final Vector<String> parameters = new Vector<String>();
					final Vector<Object> values = new Vector<Object>();
					
					for (final Map.Entry<String, Map<String, CacheValue>> nsEntry: namespaces.entrySet()){
						final String ns = nsEntry.getKey();
						final Map<String, CacheValue> cache = nsEntry.getValue();
					
						long soonestToExpire = 0;
						long latestToExpire = now;
						
						synchronized (cache){
							final Iterator<Map.Entry<String, CacheValue>> it = cache.entrySet().iterator();
							
							while (it.hasNext()){
								final Map.Entry<String, CacheValue> entry = it.next();
								
								final long expires = entry.getValue().expires; 
								
								if (expires < now){
									notifyEntryRemoved(ns, entry.getKey(), entry.getValue());
									
									it.remove();
								}
								else{
									if (soonestToExpire==0 || expires < soonestToExpire)
										soonestToExpire = expires;
								
									if (expires > latestToExpire)
										latestToExpire = expires;
								}
							}
							
							parameters.add(ns+"_size");
							values.add(Integer.valueOf(cache.size()));
						}
						
						if (soonestToExpire>0){
							parameters.add(ns+"_hours");
							values.add(Double.valueOf((latestToExpire-soonestToExpire)/(3600000d)));
						}
					}
					
					monitor.sendParameters(parameters, values);
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
	
	private static PrintWriter requestLogger = null;
	
	/**
	 * Call this one entry is removed to log the number of hits
	 * 
	 * @param namespace 
	 * @param key
	 * @param value
	 */
	static synchronized void notifyEntryRemoved(final String namespace, final String key, final CacheValue value){
		if (requestLogger==null){
			try {
				requestLogger = new PrintWriter(new FileWriter("cache.log"));
			}
			catch (IOException e) {
				return;
			}
		}
		
		requestLogger.println(System.currentTimeMillis()+" "+value.accesses+" "+namespace+" "+key);
		
		if (requestLogger.checkError())
			requestLogger = null;
	}
	
	/**
	 * @author costing
	 *
	 */
	public static final class NotifyLRUMap extends LRUMap<String, CacheValue>{
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
			
			if (ret){
				notifyEntryRemoved(namespace, eldest.getKey(), eldest.getValue());
			}
			
			return ret;
		}
		
	}
	
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
		
		ret = new NotifyLRUMap(size, name);
		
		namespaces.put(name, ret);
		
		return ret;
	}
		
	/* (non-Javadoc)
	 * @see lazyj.ExtendedServlet#execGet()
	 */
	@Override
	public void execGet() {
		final long start = System.nanoTime();
		
		execRealGet();
		
		final long duration = System.nanoTime() - start;
		
		monitor.addMeasurement("ms_to_answer", duration/1000000d);
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
						for (final Map.Entry<String, CacheValue> entryToDelete: cache.entrySet()){
							notifyEntryRemoved(entry.getKey(), entryToDelete.getKey(), entryToDelete.getValue());
						}
						
						cache.clear();
					}
				}
			}
			else{
				if (gets("ns").length()==0){				
					for (final Map.Entry<String, Map<String, CacheValue>> entry: namespaces.entrySet()){
						final Map<String, CacheValue> cache = entry.getValue();
						
						int min = -1;
						int max = 0;
						long total = 0;
						double avg ;
						
						long hits = 0;
						
						synchronized (cache){
							for (final CacheValue c: cache.values()){
								final int size = c.value.length();
								
								min = (min<0 || size<min) ? size : min;
								max = Math.max(max, size);
								total += size;
								
								hits += c.accesses;
							}
							
							avg = (double) total / cache.size();
						}
						
						pwOut.println(entry.getKey()+" : "+cache.size()+" (min: "+min+", avg: "+Format.point(avg)+", max: "+max+", total: "+Format.size(total)+") : "+hits+" hits");
					}				
				}
				else{
					final Map<String, CacheValue> cache = namespaces.get(ns);
					
					if (cache==null || cache.size()==0){
						pwOut.println("Namespace is empty");
					}
					else{
						int min = -1;
						int max = 0;
						long total = 0;
						double avg ;		
						int hits = 0;
						
						final boolean values = gets("values").length()>0;
						
						synchronized (cache){
							final ArrayList<Map.Entry<String, CacheValue>> entries = new ArrayList<Map.Entry<String,CacheValue>>(cache.entrySet());
							
							Collections.sort(entries, entryComparator);
							
							for (final Map.Entry<String, CacheValue> me: entries){
								final CacheValue cv = me.getValue();
								
								final int size = cv.value.length();
								
								min = (min<0 || size<min) ? size : min;
								max = Math.max(max, size);
								total += size;
								
								hits += cv.accesses;
								
								pwOut.println(me.getKey()+" : size "+size+", "+cv.accesses+" hits"+(values ? " : "+cv.value : ""));
							}
							
							avg = (double) total / cache.size();
						}
						
						pwOut.println("\n\n----------------\n\n"+cache.size()+" entries (min: "+min+", avg: "+Format.point(avg)+", max: "+max+", total: "+Format.size(total)+") : "+hits+" hits");
					}
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
			
			if (value.indexOf("eof")>=0){
				value = StringFactory.get(value);
				
				if (monitor != null)
					monitor.incrementCounter("SET_EOF_"+ns);
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
		
		existing.accesses++;
				
		if (monitor != null)
			monitor.incrementCounter("HIT_"+ns);
		
		if (existing.value.indexOf("eof")>=0){
			if (monitor != null)
				monitor.incrementCounter("HIT_EOF_"+ns);
		}
		
		pwOut.println(existing.value);
		pwOut.flush();
	}

}
