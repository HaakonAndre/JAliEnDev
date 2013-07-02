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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.SEfromString;
import alien.catalogue.CatalogueUtils;
import alien.catalogue.GUIDIndex;
import alien.catalogue.GUIDIndex.SEUsageStats;
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
	
	private static final void updateSECache() {
		if (!ConfigUtils.isCentralService())
			return;
		
		seCacheReadLock.lock();

		try {
			if (System.currentTimeMillis() - seCacheUpdated > CatalogueUtils.CACHE_TIMEOUT || seCache == null) {
				seCacheReadLock.unlock();

				seCacheWriteLock.lock();

				try {
					if (System.currentTimeMillis() - seCacheUpdated > CatalogueUtils.CACHE_TIMEOUT || seCache == null) {
						if (logger.isLoggable(Level.FINER)) {
							logger.log(Level.FINER, "Updating SE cache");
						}

						final DBFunctions db = ConfigUtils.getDB("alice_users");

						try{
							if (db.query("SELECT * FROM SE WHERE (seioDaemons IS NOT NULL OR seName='no_se');")) {
								final Map<Integer, SE> ses = new HashMap<Integer, SE>();
	
								while (db.moveNext()) {
									final SE se = new SE(db);
	
									if (se.size>=0)
										ses.put(Integer.valueOf(se.seNumber), se);
								}
	
								if (ses.size()>0){
									seCache = ses;
									seCacheUpdated = System.currentTimeMillis();
								}
								else{
									if (seCache==null)
										seCache = ses;
									
									// try again soon
									seCacheUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000 * 30;
								}
							}
							else {
								seCacheUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000 * 10;
							}
						}
						finally{
							db.close();
						}
					}
				}
				finally {
					seCacheWriteLock.unlock();
				}

				seCacheReadLock.lock();
			}
		} finally {
			seCacheReadLock.unlock();
		}
	}

	/**
	 * Get the SE by its number
	 * 
	 * @param seNumber
	 * @return the SE, if it exists, or <code>null</code> if it doesn't
	 */
	public static SE getSE(final int seNumber) {
		return getSE(Integer.valueOf(seNumber));
	}

	/**
	 * Get the SE by its number
	 * 
	 * @param seNumber
	 * @return the SE, if it exists, or <code>null</code> if it doesn't
	 */
	public static SE getSE(final Integer seNumber) {
		if (!ConfigUtils.isCentralService()){
			try{
				return Dispatcher.execute(new SEfromString(null, null, seNumber.intValue())).getSE();
			}
			catch (final ServerException se){
				return null;
			}
		}
		
		updateSECache();

		if (seCache == null)
			return null;

		return seCache.get(seNumber);
	}

	/**
	 * Get the SE object that has this name
	 * 
	 * @param seName
	 * @return SE, if defined, otherwise <code>null</code>
	 */
	public static SE getSE(final String seName) {
		if (seName == null || seName.length() == 0)
			return null;

		if (!ConfigUtils.isCentralService()){
			try{
				return Dispatcher.execute(new SEfromString(null, null, seName)).getSE();
			}
			catch (final ServerException se){
				return null;
			}
		}
		
		updateSECache();

		if (seCache == null)
			return null;

		final Collection<SE> ses = seCache.values();

		final String name = seName.trim().toUpperCase();

		for (final SE se : ses) {
			if (se.seName.equals(name))
				return se;
		}

		return null;
	}

	/**
	 * Get all SE objects that have the given names
	 * 
	 * @param ses names to get the objects for, can be <code>null</code> in which case all known SEs are returned 
	 * @return SE objects
	 */
	public static List<SE> getSEs(final List<String> ses) {
		updateSECache();

		if (seCache == null)
			return null;
		
		if (ses == null)
			return new ArrayList<SE>(seCache.values());
		
		final List<SE> ret = new ArrayList<SE>();
		for (final String se : ses) {
			final SE maybeSE = SEUtils.getSE(se);
			
			if (maybeSE != null)
				ret.add(maybeSE);
		}
		
		return ret;
	}

	private static Map<String, Map<Integer, Double>> seDistance = null;

	private static long seDistanceUpdated = 0;

	private static final ReentrantReadWriteLock seDistanceRWLock = new ReentrantReadWriteLock();
	private static final ReadLock seDistanceReadLock = seDistanceRWLock.readLock();
	private static final WriteLock seDistanceWriteLock = seDistanceRWLock.writeLock();

	private static final String SEDISTANCE_QUERY;
	
	static{
		if (ConfigUtils.isCentralService()){
			final DBFunctions db = ConfigUtils.getDB("alice_users");
	
			try{
				if (db.query("SELECT sitedistance FROM SEDistance LIMIT 0;", true))
					SEDISTANCE_QUERY = "SELECT sitename, senumber, sitedistance FROM SEDistance ORDER BY sitename, sitedistance;";
				else
					SEDISTANCE_QUERY = "SELECT sitename, senumber, distance FROM SEDistance ORDER BY sitename, distance;";
			}
			finally{
				db.close();
			}
			
			updateSECache();
			updateSEDistanceCache();
		}
		else{
			SEDISTANCE_QUERY = null;
		}
	}
	
	private static void updateSEDistanceCache() {
		seDistanceReadLock.lock();

		try {
			if (System.currentTimeMillis() - seDistanceUpdated > CatalogueUtils.CACHE_TIMEOUT || seDistance == null) {
				seDistanceReadLock.unlock();

				seDistanceWriteLock.lock();

				try {
					if (System.currentTimeMillis() - seDistanceUpdated > CatalogueUtils.CACHE_TIMEOUT || seDistance == null) {
						if (logger.isLoggable(Level.FINER)) {
							logger.log(Level.FINER, "Updating SE Ranks cache");
						}

						final DBFunctions db = ConfigUtils.getDB("alice_users");

						try{
							if (db.query(SEDISTANCE_QUERY)) {
								final Map<String, Map<Integer, Double>> newDistance = new HashMap<String, Map<Integer, Double>>();
	
								String sOldSite = null;
								Map<Integer, Double> oldMap = null;
	
								while (db.moveNext()) {
									final String sitename = db.gets(1).trim().toUpperCase();
									final int seNumber = db.geti(2);
									final double distance = db.getd(3);
	
									if (!sitename.equals(sOldSite) || oldMap == null) {
										oldMap = newDistance.get(sitename);
	
										if (oldMap == null) {
											oldMap = new LinkedHashMap<Integer, Double>();
											newDistance.put(sitename, oldMap);
										}
	
										sOldSite = sitename;
									}
	
									oldMap.put(Integer.valueOf(seNumber), Double.valueOf(distance));
								}
	
								if (newDistance.size()>0){
									seDistance = newDistance;
									seDistanceUpdated = System.currentTimeMillis();
								}
								else{
									if (seDistance==null)
										seDistance = newDistance;
									
									//try again soon
									seDistanceUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000 * 30;
								}
							}
							else {
								seDistanceUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000 * 10;
							}
						}
						finally{
							db.close();
						}
					}
				} finally {
					seDistanceWriteLock.unlock();
				}

				seDistanceReadLock.lock();
			}
		} finally {
			seDistanceReadLock.unlock();
		}
	}

	private static final class PFNComparatorBySite implements Serializable, Comparator<PFN> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 3852623282834261566L;

		private final Map<Integer, Double> distance;
		
		private final boolean write;

		public PFNComparatorBySite(final Map<Integer, Double> distance, final boolean write) {
			this.distance = distance;
			this.write = write;
		}

		@Override
		public int compare(final PFN o1, final PFN o2) {
			final Double distance1 = distance.get(Integer.valueOf(o1.seNumber));
			final Double distance2 = distance.get(Integer.valueOf(o2.seNumber));

			if (distance1 == null && distance2 == null) {
				// can't decide which is better, there is no ranking info for
				// either
				return 0;
			}

			if (distance1 != null && distance2 != null) {
				// both ranks known, the smallest rank goes higher
				double diff = distance1.doubleValue() - distance2.doubleValue();

				final SE se1 = getSE(o1.seNumber);
				final SE se2 = getSE(o2.seNumber);
				
				if (se1!=null && se2!=null){
					diff += write ? (se1.demoteWrite - se2.demoteWrite) : (se1.demoteRead - se2.demoteRead);
				}
				
				if (diff<0)
					return -1;
				
				if (diff>0)
					return 1;
				
				return 0;
			}

			if (distance1 != null) {
				// rank is known only for the first one, then this is better
				return -1;
			}

			// the only case left, second one is best
			return 1;
		}
	}

	/**
	 * Get all the SEs available to one site, sorted by the relative distance to
	 * the site, exclude exSEs
	 * 
	 * @param site
	 * @param write <code>true</code> for write operations, <code>false</code> for read
	 * @return sorted list of SEs based on MonALISA distance metric
	 */
	public static List<SE> getClosestSEs(final String site, final boolean write) {
		return getClosestSEs(site, null, write);
	}

	/**
	 * Get all the SEs available to one site, sorted by the relative distance to
	 * the site, exclude exSEs
	 * 
	 * @param site
	 * @param exSEs
	 * @param write <code>true</code> for write operations, <code>false</code> for read
	 * @return sorted list of SEs based on MonALISA distance metric
	 */
	public static List<SE> getClosestSEs(final String site, final List<SE> exSEs, final boolean write) {
		if (site == null || site.length() == 0)
			return getDefaultSEList(write);

		updateSEDistanceCache();

		if (seDistance == null || seDistance.size()==0)
			return getDefaultSEList(write);

		final String sitename = site.trim().toUpperCase();

		final Map<Integer, Double> distance = seDistance.get(sitename);

		if (distance == null || distance.size()==0)
			return getDefaultSEList(write);

		final List<SE> ret = new ArrayList<SE>(distance.size());

		for (final Map.Entry<Integer, Double> me : distance.entrySet()) {
			final SE se = getSE(me.getKey());

			if (se != null && (exSEs == null || !exSEs.contains(se)))
				ret.add(se);
		}
		
		Collections.sort(ret, new SEComparator(distance, write));

		return ret;
	}
	
	private static List<SE> getDefaultSEList(final boolean write){
		final List<SE> allSEs = getSEs(null);
		
		if (allSEs==null || allSEs.size()==0)
			return allSEs;
		
		final Map<Integer, Double> distance = new HashMap<Integer, Double>();
		
		final Double zero = Double.valueOf(0);
		
		for (final SE se: allSEs){
			distance.put(Integer.valueOf(se.seNumber), zero);
		}
		
		Collections.sort(allSEs, new SEComparator(distance, write));
		
		return allSEs;
	}

	/**
	 * Get if possible all SEs for a certain site with specs
	 * 
	 * @param site
	 * @param ses 
	 * @param exses 
	 * @param qos 
	 * @param write <code>true</code> for write operations, <code>false</code> for read
	 * @return the list of SEs
	 */
	public static List<SE> getBestSEsOnSpecs(final String site, final List<String> ses, final List<String> exses, final HashMap<String,Integer> qos, final boolean write) {

		if (logger.isLoggable(Level.FINE)){
			logger.log(Level.FINE, "got pos: " + ses);
			logger.log(Level.FINE, "got neg: " + exses);
			logger.log(Level.FINE, "got qos: " + qos);
		}
		
		final List<SE> SEs = SEUtils.getSEs(ses);
		
		final List<SE> exSEs = SEUtils.getSEs(exses);
		
		SEs.removeAll(exSEs);

		exSEs.addAll(SEs);		
				
		for (final Map.Entry<String, Integer> qosDef : qos.entrySet()) {
			if (qosDef.getValue().intValue() > 0) {

				// TODO: get a number #qos.get(qosType) of qosType SEs
				final List<SE> discoveredSEs = SEUtils.getClosestSEs(site, exSEs, write);

				final Iterator<SE> it = discoveredSEs.iterator();

				int counter = 0;
				
				while (counter < qosDef.getValue().intValue() && it.hasNext()) {
					final SE se = it.next();
					
					if (!se.isQosType(qosDef.getKey()) || exSEs.contains(se))
						continue;

					SEs.add(se);
					counter++;
				}
			}
		}
		
		if (logger.isLoggable(Level.FINE))
			logger.log(Level.FINE, "Returning SE list: " + SEs);

		return SEs;
	}

	/**
	 * Sort a collection of PFNs by their relative distance to a given site
	 * (where the job is running for example)
	 * 
	 * @param pfns
	 * @param sSite
	 * @param removeBrokenSEs
	 * @param write <code>true</code> for write operations, <code>false</code> for read
	 * @return the sorted list of locations
	 */
	public static List<PFN> sortBySite(final Collection<PFN> pfns, final String sSite, final boolean removeBrokenSEs, final boolean write) {
		if (pfns == null)
			return null;

		final List<PFN> ret = new ArrayList<PFN>(pfns);

		if (ret.size() <= 1 || sSite == null || sSite.length() == 0)
			return ret;

		updateSEDistanceCache();

		if (seDistance == null)
			return null;

		final Map<Integer, Double> ranks = seDistance.get(sSite.trim().toUpperCase());

		if (ranks == null)
			return ret;

		if (removeBrokenSEs) {
			final Iterator<PFN> it = ret.iterator();

			while (it.hasNext()) {
				final PFN pfn = it.next();

				if (!ranks.containsKey(Integer.valueOf(pfn.seNumber)))
					it.remove();
			}
		}

		final Comparator<PFN> c = new PFNComparatorBySite(ranks, write);

		Collections.sort(ret, c);

		return ret;
	}

	/**
	 * Sort a collection of PFNs by their relative distance to a given site
	 * (where the job is running for example), priorize SEs, exclude exSEs
	 * 
	 * @param pfns
	 * @param sSite
	 * @param removeBrokenSEs
	 * @param SEs
	 * @param exSEs
	 * @param write <code>true</code> for write operations, <code>false</code> for read
	 * @return the sorted list of locations
	 */
	public static List<PFN> sortBySiteSpecifySEs(final Collection<PFN> pfns, final String sSite, final boolean removeBrokenSEs, final List<SE> SEs, final List<SE> exSEs, final boolean write) {
		List<PFN> spfns = sortBySite(pfns, sSite, removeBrokenSEs, write);

		if ((SEs==null || SEs.isEmpty()) && (exSEs==null || exSEs.isEmpty()))
			return spfns;
		
		List<PFN> tail = new ArrayList<PFN>(spfns.size());
		List<PFN> ret = new ArrayList<PFN>(spfns.size());
		
		for (PFN pfn : spfns) {
			if (SEs != null && SEs.contains(SEUtils.getSE(pfn.seNumber)))
				ret.add(pfn);
			else 
			if (exSEs == null || !exSEs.contains(SEUtils.getSE(pfn.seNumber)))
				tail.add(pfn);				
		}

		ret.addAll(tail);
		return ret;
	}

	/**
	 * @author costing
	 * @since Nov 14, 2010
	 */
	public static final class SEComparator implements Comparator<SE>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -5231000693345849547L;
		
		private final Map<Integer, Double> distance;

		private final boolean write;
		
		/**
		 * @param distance
		 * @param write <code>true</code> for write operations, <code>false</code> for read
		 */
		public SEComparator(final Map<Integer, Double> distance, final boolean write) {
			this.distance = distance;
			this.write = write;
		}

		@Override
		public int compare(final SE o1, final SE o2) {
			final Double d1 = distance.get(Integer.valueOf(o1.seNumber));
			final Double d2 = distance.get(Integer.valueOf(o2.seNumber));

			// broken first SE, move the second one to the front if it's ok
			if (d1 == null) {
				return d2 == null ? 0 : 1;
			}

			// broken second SE, move the first one to the front
			if (d2 == null)
				return -1;

			// lower rank to the front

			final double rank1 = d1.doubleValue() + (write ? o1.demoteWrite : o1.demoteRead);
			final double rank2 = d2.doubleValue() + (write ? o2.demoteWrite : o2.demoteRead);
			
			double diff = rank1 - rank2;
			
			if (diff<0)
				return -1;
			
			if (diff>0)
				return 1;
			
			return 0;
		}
	}

	/**
	 * @param ses
	 * @param sSite
	 * @param removeBrokenSEs
	 * @param write <code>true</code> for write operations, <code>false</code> for read
	 * @return the sorted list of SEs
	 */
	public static List<SE> sortSEsBySite(final Collection<SE> ses, final String sSite, final boolean removeBrokenSEs, final boolean write) {
		if (ses == null)
			return null;

		final List<SE> ret = new ArrayList<SE>(ses);

		if ((ret.size() <= 1 || sSite == null || sSite.length() == 0)
				&& (!removeBrokenSEs))
			return ret;

		updateSEDistanceCache();

		if (seDistance == null)
			return null;

		final Map<Integer, Double> ranks = sSite != null ? seDistance.get(sSite.trim().toUpperCase()) : null;

		if (ranks == null) {
			// missing information about this site, leave the storages as they
			// are
			return ret;
		}

		if (removeBrokenSEs) {
			final Iterator<SE> it = ret.iterator();

			while (it.hasNext()) {
				final SE se = it.next();

				if (!ranks.containsKey(Integer.valueOf(se.seNumber)))
					it.remove();
			}
		}

		final Comparator<SE> c = new SEComparator(ranks, write);

		Collections.sort(ret, c);

		return ret;
	}
	
	/**
	 * Get the distance between a site and a target SE
	 * 
	 * @param sSite reference site
	 * @param toSE target se, can be either a {@link SE} object, a name (as String) or a SE number (Integer), anything else will throw an exception
	 * @param write <code>true</code> for writing, <code>false</code> for reading
	 * @return the distance (0 = local, 1 = far away, with negative values being strongly preferred and >1 values highly demoted)
	 */
	public static Double getDistance(final String sSite, final Object toSE, final boolean write){
		if (toSE == null)
			return null;
		
		final SE se;
		
		if (toSE instanceof SE)
			se = (SE) toSE;
		else
		if (toSE instanceof String)
			se = getSE((String) toSE);
		else
		if (toSE instanceof Integer)
			se = getSE((Integer) toSE);
		else
			throw new IllegalArgumentException("Invalid object type for the toSE parameter: "+toSE.getClass().getCanonicalName());
		
		if (se==null)
			return null;
		
		updateSEDistanceCache();
		
		if (seDistance == null)
			return null;
		
		final Map<Integer, Double> ranks = seDistance.get(sSite.trim().toUpperCase());
		
		if (ranks == null)
			return null;
		
		final Double distance = ranks.get(Integer.valueOf(se.seNumber));
		
		if (distance == null)
			return null;
		
		final double d = distance.doubleValue() + (write ? se.demoteWrite : se.demoteRead);
		
		return Double.valueOf(d);
	}

	/**
	 * Update the number of files and the total size for each known SE, according to the G*L and G*L_PFN tables
	 */
	public static void updateSEUsageCache(){
		final Map<Integer, SEUsageStats> m = getSEUsage();
		
		final DBFunctions db = ConfigUtils.getDB("alice_users");
		
		try{
			for (final Map.Entry<Integer, SEUsageStats> entry: m.entrySet()){
				db.query("UPDATE SE SET seUsedSpace=?, seNumFiles=? WHERE seNumber=?;", false, Long.valueOf(entry.getValue().usedSpace), Long.valueOf(entry.getValue().fileCount), entry.getKey());
				
				final SE se = getSE(entry.getKey().intValue());
				
				if (se!=null){
					se.seUsedSpace = entry.getValue().usedSpace;
					se.seNumFiles = entry.getValue().fileCount;
				}
			}
		}
		finally{
			db.close();
		}
	}
	
	private static Map<Integer, SEUsageStats> getSEUsage(){
		final Map<Integer, SEUsageStats> m = new HashMap<Integer, SEUsageStats>(); 
		
		for (final GUIDIndex index: CatalogueUtils.getAllGUIDIndexes()){
			System.err.println("Getting usage from "+index);
			
			final Map<Integer, SEUsageStats> t = index.getSEUsageStats();
			
			for (final Map.Entry<Integer, SEUsageStats> entry: t.entrySet()){
				final SEUsageStats s = m.get(entry.getKey());
				
				if (s==null){
					m.put(entry.getKey(), entry.getValue());
				}
				else{
					s.merge(entry.getValue());
				}
			}
		}
		
		return m;
	}
	
	/**
	 * Debug method
	 * 
	 * @param args
	 */
	public static void main(final String[] args) {
	    updateSEUsageCache();
	}
}
