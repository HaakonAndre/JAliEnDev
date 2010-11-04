/**
 * 
 */
package alien.quotas;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import alien.config.ConfigUtils;

import lazyj.DBFunctions;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public final class QuotaUtilities {

	private static Map<String, FQuota> fileQuotas = null;
	private static long fileQuotasLastUpdated = 0;
	
	private static synchronized void updateFileQuotasCache(){
		if (System.currentTimeMillis() - fileQuotasLastUpdated > 1000*60*5 || fileQuotas==null){
			final Map<String, FQuota> newQuotas = new HashMap<String, FQuota>();
			
			final DBFunctions db = ConfigUtils.getDB("?");
			
			db.query("?");
			
			while (db.moveNext()){
				final FQuota fq = new FQuota(db);
		
				if (fq.account!=null)
					newQuotas.put(fq.account.toLowerCase(), fq);
			}
			
			fileQuotas = Collections.unmodifiableMap(newQuotas);
			fileQuotasLastUpdated = System.currentTimeMillis();
		}
	}
	
	/**
	 * Get the file quota for a particular account
	 * 
	 * @param account
	 * @return file quota
	 */
	public static FQuota getFQuota(final String account){
		if (account==null || account.length()==0)
			return null;
		
		updateFileQuotasCache();
		
		return fileQuotas.get(account.toLowerCase());
	}


	private static Map<String, JQuota> jobQuotas = null;
	private static long jobQuotasLastUpdated = 0;
	
	private static synchronized void updateJobQuotasCache(){
		if (System.currentTimeMillis() - jobQuotasLastUpdated > 1000*60*5 || jobQuotas==null){
			final Map<String, JQuota> newQuotas = new HashMap<String, JQuota>();
			
			final DBFunctions db = ConfigUtils.getDB("?");
			
			db.query("?");
			
			while (db.moveNext()){
				final JQuota jq = new JQuota(db);
		
				if (jq.account!=null)
					newQuotas.put(jq.account.toLowerCase(), jq);
			}
			
			jobQuotas = Collections.unmodifiableMap(newQuotas);
			jobQuotasLastUpdated = System.currentTimeMillis();
		}
	}
	
	/**
	 * Get the job quota for a particular account
	 * 
	 * @param account
	 * @return job quota
	 */
	public static FQuota getJQuota(final String account){
		if (account==null || account.length()==0)
			return null;
		
		updateJobQuotasCache();
		
		return fileQuotas.get(account.toLowerCase());
	}
}
