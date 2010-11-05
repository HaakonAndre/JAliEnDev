/**
 * 
 */
package alien.quotas;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

import lazyj.DBFunctions;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public final class QuotaUtilities {
	static transient final Logger logger = ConfigUtils.getLogger(QuotaUtilities.class.getCanonicalName());

	private static Map<String, Quota> quotas = null;
	private static long quotasLastUpdated = 0;
	
	private static synchronized void updateFileQuotasCache(){
		if (System.currentTimeMillis() - quotasLastUpdated > 1000*60*2 || quotas==null){
			final Map<String, Quota> newQuotas = new HashMap<String, Quota>();
			
			final DBFunctions db = ConfigUtils.getDB("processes");
			
			db.query("SELECT * FROM PRIORITY;");
			
			while (db.moveNext()){
				final Quota fq = new Quota(db);
		
				if (fq.user!=null)
					newQuotas.put(fq.user.toLowerCase(), fq);
			}
			
			quotas = Collections.unmodifiableMap(newQuotas);
			quotasLastUpdated = System.currentTimeMillis();
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
		
		return quotas.get(account.toLowerCase());
	}
}
