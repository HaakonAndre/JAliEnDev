/**
 * 
 */
package alien.quotas;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.CatalogueUtils;
import alien.config.ConfigUtils;

import lazyj.DBFunctions;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public final class QuotaUtilities {
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(QuotaUtilities.class.getCanonicalName());

	private static Map<String, Quota> quotas = null;
	private static long quotasLastUpdated = 0;
	
	private static final ReentrantReadWriteLock quotasRWLock = new ReentrantReadWriteLock();
	private static final ReadLock quotaReadLock = quotasRWLock.readLock();
	private static final WriteLock quotaWriteLock = quotasRWLock.writeLock();
	
	private static void updateFileQuotasCache(){
		quotaReadLock.lock();
		
		try{
			if (System.currentTimeMillis() - quotasLastUpdated > CatalogueUtils.CACHE_TIMEOUT || quotas==null){
				quotaReadLock.unlock();
				
				quotaWriteLock.lock();
				
				try{
					if (System.currentTimeMillis() - quotasLastUpdated > CatalogueUtils.CACHE_TIMEOUT || quotas==null){
						if (logger.isLoggable(Level.FINER)){
							logger.log(Level.FINER, "Updating Quotas cache");
						}
						
						final DBFunctions db = ConfigUtils.getDB("processes");
					
						if (db.query("SELECT * FROM PRIORITY;")){
							final Map<String, Quota> newQuotas = new HashMap<String, Quota>();
							
							while (db.moveNext()){
								final Quota fq = new Quota(db);
					
								if (fq.user!=null)
									newQuotas.put(fq.user.toLowerCase(), fq);
							}
						
							quotas = Collections.unmodifiableMap(newQuotas);
							quotasLastUpdated = System.currentTimeMillis();
						}
						else{
							quotasLastUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000*10;
						}
					}
				}
				finally{
					quotaWriteLock.unlock();
				}
				
				quotaReadLock.lock();
			}
		}
		finally{
			quotaReadLock.unlock();
		}
	}
	
	/**
	 * Get the file quota for a particular account
	 * 
	 * @param account
	 * @return file quota
	 */
	public static Quota getFQuota(final String account){
		if (account==null || account.length()==0)
			return null;
		
		updateFileQuotasCache();
		
		if (quotas==null)
			return null;
		
		return quotas.get(account.toLowerCase());
	}
}
