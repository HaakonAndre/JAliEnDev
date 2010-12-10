/**
 * 
 */
package alien.se;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import alien.catalogue.CatalogueUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public final class SEUtils {
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(SEUtils.class.getCanonicalName());

	private static Map<Integer, SE> seCache = null;
	
	private static long seCacheUpdated = 0;
	
	private static final ReentrantReadWriteLock seCacheRWLock = new ReentrantReadWriteLock();
	private static final ReadLock seCacheReadLock = seCacheRWLock.readLock();
	private static final WriteLock seCacheWriteLock = seCacheRWLock.writeLock();
	
	private static final void updateSECache(){
		seCacheReadLock.lock();
		
		try{
			if (System.currentTimeMillis() - seCacheUpdated > CatalogueUtils.CACHE_TIMEOUT || seCache == null){
				seCacheReadLock.unlock();
				
				seCacheWriteLock.lock();
				
				try{
					if (System.currentTimeMillis() - seCacheUpdated > CatalogueUtils.CACHE_TIMEOUT || seCache == null){
						if (logger.isLoggable(Level.FINER)){
							logger.log(Level.FINER, "Updating SE cache");
						}
						
						final DBFunctions db = ConfigUtils.getDB("alice_users");
					
						if (db.query("SELECT * FROM SE;")){
							final Map<Integer, SE> ses = new HashMap<Integer, SE>();
												
							while (db.moveNext()){
								final SE se = new SE(db);
							
								ses.put(Integer.valueOf(se.seNumber), se);
							}
						
							seCache = ses;
							seCacheUpdated = System.currentTimeMillis();
						}
						else{
							seCacheUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000*10;
						}
					}
				}
				finally{
					seCacheWriteLock.unlock();
				}
				
				seCacheReadLock.lock();
			}
		}
		finally{
			seCacheReadLock.unlock();
		}
	}
	
	/**
	 * Get the SE by its number
	 * 
	 * @param seNumber
	 * @return the SE, if it exists, or <code>null</code> if it doesn't
	 */
	public static SE getSE(final int seNumber){
		return getSE(Integer.valueOf(seNumber));
	}
	
	/**
	 * Get the SE by its number
	 * 
	 * @param seNumber
	 * @return the SE, if it exists, or <code>null</code> if it doesn't
	 */
	public static SE getSE(final Integer seNumber){
		updateSECache();
		
		if (seCache==null)
			return null;
		
		return seCache.get(seNumber);
	}
	
	/**
	 * Get the SE object that has this name
	 * 
	 * @param seName
	 * @return SE, if defined, otherwise <code>null</code>
	 */
	public static SE getSE(final String seName){
		if (seName==null || seName.length()==0)
			return null;
		
		updateSECache();
		
		if (seCache==null)
			return null;
		
		final Collection<SE> ses = seCache.values();
		
		final String name = seName.trim().toUpperCase();
		
		for (final SE se: ses){
			if (se.seName.equals(name))
				return se;
		}
		
		return null;
	}
	
	/**
	 * Get all SE objects that have the given names
	 * 
	 * @param seNames
	 * @return SE objects
	 */
	public static Map<String, SE> getSEs(final Set<String> seNames){
		if (seNames==null)
			return null;
		
		final Map<String, SE> ret = new HashMap<String, SE>(seNames.size()); 
		
		for (final String name: seNames){
			final SE se = getSE(name);
			
			if (se!=null)
				ret.put(name, se);
		}
		
		return ret;
	}
	
	private static Map<String, Map<Integer, Integer>> seRanks = null;
	
	private static long seRanksUpdated = 0;
	
	private static final ReentrantReadWriteLock seRanksRWLock = new ReentrantReadWriteLock();
	private static final ReadLock seRanksReadLock = seRanksRWLock.readLock();
	private static final WriteLock seRanksWriteLock = seRanksRWLock.writeLock();
	
	private static void updateSERanksCache(){
		seRanksReadLock.lock();
		
		try{
			if (System.currentTimeMillis() - seRanksUpdated > CatalogueUtils.CACHE_TIMEOUT || seRanks == null){
				seRanksReadLock.unlock();
				
				seRanksWriteLock.lock();
	
				try{
					if (System.currentTimeMillis() - seRanksUpdated > CatalogueUtils.CACHE_TIMEOUT || seRanks == null){
						if (logger.isLoggable(Level.FINER)){
							logger.log(Level.FINER, "Updating SE Ranks cache");
						}
						
						final DBFunctions db = ConfigUtils.getDB("alice_users");
						
						if (db.query("SELECT sitename, seNumber, rank FROM SERanks ORDER BY sitename, rank;")){
							final Map<String, Map<Integer, Integer>> newRanks = new HashMap<String, Map<Integer,Integer>>();
							
							String sOldSite = null;
							Map<Integer, Integer> oldMap = null;
							
							while (db.moveNext()){
								final String sitename = db.gets(1).trim().toUpperCase();
								final int seNumber = db.geti(2);
								final int rank = db.geti(3);
								
								if (!sitename.equals(sOldSite) || oldMap==null){
									oldMap = newRanks.get(sitename);
									
									if (oldMap==null){
										oldMap = new LinkedHashMap<Integer, Integer>();
										newRanks.put(sitename, oldMap);
									}
									
									sOldSite=sitename;
								}
								
								oldMap.put(Integer.valueOf(seNumber), Integer.valueOf(rank));
							}
							
							seRanks = newRanks;
							seRanksUpdated = System.currentTimeMillis();
						}
						else{
							seRanksUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000*10;
						}
					}
				}
				finally{
					seRanksWriteLock.unlock();
				}
				
				seRanksReadLock.lock();
			}
		}
		finally{
			seRanksReadLock.unlock();
		}
	}
	
	private static final class PFNComparatorBySite implements Serializable, Comparator<PFN>{
		/**
		 * 
		 */
		private static final long serialVersionUID = 3852623282834261566L;
		
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
	 * Get all the SEs available to one site, sorted by the relative distance to the site
	 * 
	 * @param site
	 * @return sorted list of SEs based on MonALISA distance metric
	 */
	public static List<SE> getClosestSEs(final String site){
		if (site==null || site.length()==0)
			return null;
				
		updateSERanksCache();
		
		if (seRanks==null)
			return null;
		
		final String sitename = site.trim().toUpperCase();
		
		final Map<Integer, Integer> ranks = seRanks.get(sitename);
		
		if (ranks==null)
			return null;
		
		final List<SE> ret = new ArrayList<SE>(ranks.size());
		
		for (final Map.Entry<Integer, Integer> me: ranks.entrySet()){
			final SE se = getSE(me.getKey());
			
			if (se!=null)
				ret.add(se);
		}
		
		// We don't need to sort the return list because of the way the cache is built
		// (it sorts by sitename and rank) and because the cache is LinkedHashMap
		
		return ret;
	}
	
	/**
	 * Sort a collection of PFNs by their relative distance to a given site (where the job is running for example) 
	 * 
	 * @param pfns
	 * @param sSite
	 * @param removeBrokenSEs 
	 * @return the sorted list of locations
	 */
	public static List<PFN> sortBySite(final Collection<PFN> pfns, final String sSite, final boolean removeBrokenSEs){
		if (pfns==null)
			return null;
		
		final List<PFN> ret = new ArrayList<PFN>(pfns);
		
		if (ret.size()<=1 || sSite==null || sSite.length()==0)
			return ret;
		
		updateSERanksCache();
		
		if (seRanks==null)
			return null;
		
		final Map<Integer, Integer> ranks = seRanks.get(sSite.trim().toUpperCase());
		
		if (ranks==null)
			return ret;
	
		if (removeBrokenSEs){
			final Iterator<PFN> it = ret.iterator();
			
			while (it.hasNext()){
				final PFN pfn = it.next();
				
				if (!ranks.containsKey(Integer.valueOf(pfn.seNumber)))
					it.remove();
			}
		}
		
		final Comparator<PFN> c = new PFNComparatorBySite(ranks);
		
		Collections.sort(ret, c);
		
		return ret;
	}
	
	/**
	 * @author costing
	 * @since Nov 14, 2010
	 */
	public static final class SEComparator implements Comparator<SE> {
		private final Map<Integer, Integer> ranks;
		
		/**
		 * @param ranks 
		 */
		public SEComparator(final Map<Integer, Integer> ranks) {
			this.ranks = ranks;
		}

		@Override
		public int compare(final SE o1, final SE o2) {
			final Integer r1 = ranks.get(Integer.valueOf(o1.seNumber));
			final Integer r2 = ranks.get(Integer.valueOf(o2.seNumber));
			
			// broken first SE, move the second one to the front if it's ok
			if (r1==null){
				return r2 == null ? 0 : 1;
			}
			
			// broken second SE, move the first one to the front
			if (r2==null)
				return -1;
			
			// lower rank to the front
			
			return r1.compareTo(r2);
		}
	}
	
	/**
	 * @param ses
	 * @param sSite
	 * @param removeBrokenSEs
	 * @return the sorted list of SEs
	 */
	public static List<SE> sortSEsBySite(final Collection<SE> ses, final String sSite, final boolean removeBrokenSEs){
		if (ses==null)
			return null;
		
		final List<SE> ret = new ArrayList<SE>(ses);
		
		if ((ret.size()<=1 || sSite==null || sSite.length()==0) && (!removeBrokenSEs))
			return ret;
		
		updateSERanksCache();
		
		if (seRanks==null)
			return null;
		
		final Map<Integer, Integer> ranks = sSite!=null ? seRanks.get(sSite.trim().toUpperCase()) : null;
		 
		if (ranks==null){
			// missing information about this site, leave the storages as they are
			return ret;
		}
		
		if (removeBrokenSEs){
			final Iterator<SE> it = ret.iterator();
			
			while (it.hasNext()){
				final SE se = it.next();
				
				if (!ranks.containsKey(Integer.valueOf(se.seNumber)))
					it.remove();
			}
		}
		
		final Comparator<SE> c = new SEComparator(ranks);
		
		Collections.sort(ret, c);
		
		return ret;		
	}
	
}
