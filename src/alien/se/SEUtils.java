/**
 * 
 */
package alien.se;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public final class SEUtils {
	static transient final Logger logger = ConfigUtils.getLogger(SEUtils.class.getCanonicalName());

	private static Map<Integer, SE> seCache = null;
	
	private static long seCacheUpdated = 0;
	
	private static synchronized final void updateSECache(){
		if (System.currentTimeMillis() - seCacheUpdated > 1000*60*5 || seCache == null){
			final Map<Integer, SE> ses = new HashMap<Integer, SE>();
			
			final DBFunctions db = ConfigUtils.getDB("alice_users");
			
			db.query("SELECT * FROM SE;");
			
			while (db.moveNext()){
				final SE se = new SE(db);
				
				ses.put(Integer.valueOf(se.seNumber), se);
			}
			
			seCache = ses;
			seCacheUpdated = System.currentTimeMillis();
		}
	}
	
	/**
	 * Get the SE by its number
	 * 
	 * @param seNumber
	 * @return the SE, if it exists, or <code>null</code> if it doesn't
	 */
	public SE getSE(final int seNumber){
		updateSECache();
		
		return seCache.get(Integer.valueOf(seNumber));
	}
	
	private static Map<String, Map<Integer, Integer>> seRanks = null;
	
	private static long seRanksUpdated = 0;
	
	private static synchronized void updateSERanksCache(){
		if (System.currentTimeMillis() - seRanksUpdated > 1000*60*10 || seRanks == null){
			final Map<String, Map<Integer, Integer>> newRanks = new HashMap<String, Map<Integer,Integer>>();
			
			String sOldSite = null;
			Map<Integer, Integer> oldMap = null;
			
			final DBFunctions db = ConfigUtils.getDB("alice_users");
			
			db.query("SELECT sitename, seNumber, rank FROM SERanks ORDER BY sitename;");
			
			while (db.moveNext()){
				final String sitename = db.gets(1).trim().toUpperCase();
				final int seNumber = db.geti(2);
				final int rank = db.geti(3);
				
				if (!sitename.equals(sOldSite) || oldMap==null){
					oldMap = newRanks.get(sitename);
					
					if (oldMap==null){
						oldMap = new HashMap<Integer, Integer>();
						newRanks.put(sitename, oldMap);
					}
					
					sOldSite=sitename;
				}
				
				oldMap.put(Integer.valueOf(seNumber), Integer.valueOf(rank));
			}
			
			seRanks = newRanks;
			seRanksUpdated = System.currentTimeMillis();
		}
	}
	
	private static final class PFNComparatorBySite implements Comparator<PFN>{
		private final Map<Integer, Integer> ranks;
		
		public PFNComparatorBySite(final Map<Integer, Integer> ranks){
			this.ranks = ranks;
		}

		@Override
		public int compare(final PFN o1, final PFN o2) {
			final Integer rank1 = ranks.get(Integer.valueOf(o1.seNumber));
			final Integer rank2 = ranks.get(Integer.valueOf(o2.seNumber));
			
			if (rank1==null && rank2==null){
				// can't decide which is better, there is no ranking info for either
				return 0;
			}
			
			if (rank1!=null && rank2!=null){
				// both ranks known, the smallest rank goes higher
				return rank1.intValue() - rank2.intValue();
			}
			
			if (rank1!=null){
				// rank is known only for the first one, then this is better
				return -1; 
			}
			
			// the only case left, second one is best
			return 1;
		}
	}
	
	/**
	 * Sort a collection of PFNs by their relative distance to a given site (where the job is running for example) 
	 * 
	 * @param pfns
	 * @param sSite
	 * @return the sorted list of locations
	 */
	public static List<PFN> sortBySite(final Collection<PFN> pfns, final String sSite){
		if (pfns==null)
			return null;
		
		final List<PFN> ret = new ArrayList<PFN>(pfns);
		
		if (ret.size()<=1 || sSite==null || sSite.length()==0)
			return ret;
		
		updateSERanksCache();
		
		final Map<Integer, Integer> ranks = seRanks.get(sSite.trim().toUpperCase());
		
		if (ranks==null)
			return ret;
		
		final Comparator<PFN> c = new PFNComparatorBySite(ranks);
		
		Collections.sort(ret, c);
		
		return ret;
	}
	
}
