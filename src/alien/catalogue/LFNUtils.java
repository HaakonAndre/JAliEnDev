package alien.catalogue;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * LFN utilities
 * 
 * @author costing
 *
 */
public class LFNUtils {
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(LFNUtils.class.getCanonicalName());
	
	/**
	 * Get the LFN entry for this catalog filename
	 * 
	 * @param fileName
	 * @return LFN entry
	 */
	public static LFN getLFN(final String fileName){
		return getLFN(fileName, false);
	}

	/**
	 * Get the LFN entry for this catalog filename, optionally returning an empty object
	 * if the entry doesn't exist (yet)
	 * 
	 * @param fileName
	 * @param evenIfDoesntExist
	 * @return entry
	 */
	public static LFN getLFN(final String fileName, final boolean evenIfDoesntExist){
		if (fileName == null || fileName.length() == 0)
			return null;
		
		
		String processedFileName = fileName;
		
		while (processedFileName.indexOf("//")>=0)
			processedFileName = Format.replace(processedFileName, "//", "/");
		
		final IndexTableEntry ite = CatalogueUtils.getClosestMatch(fileName);
		
		if (ite==null){
			logger.log(Level.FINE, "IndexTableEntry is null for: "+fileName+" (even if doesn't exist: "+evenIfDoesntExist+")");
			
			return null;
		}
		
		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "Using "+ite+" for: "+fileName);
		
		return ite.getLFN(fileName, evenIfDoesntExist);
	}
	
	/**
	 * Make sure the parent directory exists
	 * 
	 * @param lfn
	 * @return the updated LFN entry
	 */
	static LFN ensureDir(final LFN lfn){
		if (lfn.exists)
			return lfn;
		
		LFN parent = lfn.getParentDir(true);
		
		if (!parent.exists){
			parent.owner = lfn.owner;
			parent.gowner = lfn.gowner;
			parent = ensureDir(parent);
		}
		
		if (parent == null)
			return null;
		
		lfn.parentDir = parent;
		lfn.type = 'd';
		
		if (insertLFN(lfn))
			return lfn;
		
		return null;
	}
	
	/**
	 * Insert an LFN in the catalogue
	 * 
	 * @param lfn
	 * @return true if the entry was inserted (or previously existed), false if there was an error
	 */
	static boolean insertLFN(final LFN lfn){
		if (lfn.exists){
			// nothing to be done, the entry already exists
			return true;
		}
		
		final IndexTableEntry ite = CatalogueUtils.getClosestMatch(lfn.getCanonicalName());
		
		if (ite==null){
			logger.log(Level.WARNING, "IndexTableEntry is null for: "+lfn.getCanonicalName());
			
			return false;
		}
		
		final LFN parent = ensureDir(lfn.getParentDir());
		
		if (parent==null){
			logger.log(Level.WARNING, "Parent dir is null for "+lfn.getCanonicalName());
			
			return false;
		}
		
		lfn.parentDir = parent;
		lfn.indexTableEntry = ite;
		
		if (lfn.indexTableEntry.equals(parent.indexTableEntry))
			lfn.dir = parent.entryId;
		
		return lfn.insert();
	}
	
	/**
	 * the "-s" flag of AliEn `find`
	 */
	public static final int FIND_NO_SORT = 1;
	
	/**
	 * the "-d" flag of AliEn `find`
	 */
	public static final int FIND_INCLUDE_DIRS = 2;
	
	/**
	 * @param path
	 * @param pattern
	 * @param flags a combination of FIND_* flags
	 * @return the list of LFNs that match
	 */
	public static List<LFN> find(final String path, final String pattern, final int flags){
		final List<LFN> ret = new ArrayList<LFN>();
		
		final List<IndexTableEntry> matchingTables = CatalogueUtils.getAllMatchingTables(path);

		final String processedPattern = Format.replace(pattern, "*", "%");
		
		for (final IndexTableEntry ite: matchingTables){
			final List<LFN> findResults = ite.find(path, processedPattern, flags);
			
			if (findResults!=null && findResults.size()>0)
				ret.addAll(findResults);
		}
		
		return ret;		
	}
	
	/**
	 * Create a new collection with the given path
	 * 
	 * @param collectionName full path (LFN) of the collection
	 * @param owner collection owner
	 * @return the newly created collection
	 */
	public static LFN createCollection(final String collectionName, final AliEnPrincipal owner){
		if (collectionName==null || owner==null)
			return null;
		
		final LFN lfn = getLFN(collectionName, true);
		
		if (lfn.exists)
			return null;
		
		LFN parentDir = lfn.getParentDir();
		
		if (parentDir==null){
			// will not create directories up to this path, do it explicitly before calling this
			return null;
		}
		
		if (!AuthorizationChecker.canWrite(parentDir, owner)){
			// not allowed to write here. Not sure we should double check here, but it doesn't hurt to be sure
			return null;
		}
		
		final GUID guid = GUIDUtils.createGuid();
		
		guid.ctime = lfn.ctime = new Date();
		guid.owner = lfn.owner = owner.getName();
		
		final Set<String> roles = owner.getRoles();
		guid.gowner = lfn.gowner = (roles!=null && roles.size() > 0) ? roles.iterator().next() : lfn.owner;
		guid.size = lfn.size = 0;
		guid.type = lfn.type = 'c';
		
		lfn.guid = guid.guid;
		lfn.perm = guid.perm = "755";
		lfn.aclId = guid.aclId = -1;
		lfn.jobid = -1;
		lfn.md5 = guid.md5 = "n/a";
		
		if (!guid.update())
			return null;
		
		if (!insertLFN(lfn))
			return null;
		
		final DBFunctions db = ConfigUtils.getDB("alice_data");

		final String q = "INSERT INTO COLLECTIONS (collGUID) VALUES (string2binary('"+lfn.guid.toString()+"'));"; 
		
		if (!db.query(q))
			return null;
		
		return lfn;
	}
	
	/**
	 * @param collection
	 * @param lfns
	 * @return <code>true</code> if anything was changed
	 */
	public static boolean addToCollection(final LFN collection, final Set<LFN> lfns){
		final DBFunctions db = ConfigUtils.getDB("alice_data");
		
		if (!collection.exists || !collection.isCollection() || lfns==null || lfns.size()==0){
			return false;
		}
		
		final Set<String> currentLFNs = collection.listCollection();
		
		db.query("SELECT collectionId FROM COLLECTIONS where collGUID=string2binary('"+collection.guid.toString()+"');");
		
		if (!db.moveNext())
			return false;
		
		final int collectionId = db.geti(1);
		
		final Set<LFN> toAdd = new LinkedHashSet<LFN>();
		
		for (final LFN lfn: lfns){
			if (currentLFNs.contains(lfn.getCanonicalName()))
				continue;
			
			toAdd.add(lfn);
		}
		
		if (toAdd.size()==0)
			return false;
		
		final GUID guid = GUIDUtils.getGUID(collection.guid);
		
		Set<Integer> commonSEs = guid.size==0 ? null : new HashSet<Integer>(guid.seStringList);
			
		for (final LFN lfn: toAdd){
			if (commonSEs==null || commonSEs.size()>0){
				final Set<PFN> pfns = lfn.whereisReal();
				
				final Set<Integer> ses = new HashSet<Integer>();
		
				for (PFN pfn: pfns){
					ses.add(Integer.valueOf(pfn.seNumber));
				}
				
				if (ses.size()>0){
					if (commonSEs!=null)
						commonSEs.retainAll(ses);
					else
						commonSEs = ses;
				}
			}
			
			if (db.query("INSERT INTO COLLECTIONS_ELEM (collectionId,origLFN,guid) VALUES ("+collectionId+", '"+Format.escSQL(lfn.getCanonicalName())+"', string2binary('"+lfn.guid.toString()+"'));")){
				guid.size += lfn.size;
			}
		}
		
		if (collection.size == guid.size)
			return false;	// nothing changed
		
		if (commonSEs!=null)
			guid.seStringList = commonSEs;
		
		collection.size = guid.size;
		
		collection.ctime = guid.ctime = new Date();
		
		guid.update();
		collection.update();
		
		return true;
	}
	
}
