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
import lazyj.StringFactory;
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
		
		processedFileName = Format.replace(processedFileName, "/./", "/");

		int idx = processedFileName.indexOf("/../");
		
		while (idx>0){
			int idx2 = processedFileName.lastIndexOf("/", idx-1);
			
			if (idx2>0){
				processedFileName = processedFileName.substring(0, idx2) + processedFileName.substring(idx+3);
			}
			
//			System.err.println("After replacing .. : "+processedFileName);
			
			idx = processedFileName.indexOf("/../");
		}
		
		if (processedFileName.endsWith("/..")){
			int idx2 = processedFileName.lastIndexOf('/', processedFileName.length()-4);
			
			if (idx2>0)
				processedFileName = processedFileName.substring(0, idx2);
		}
		
		final IndexTableEntry ite = CatalogueUtils.getClosestMatch(processedFileName);
		
		if (ite==null){
			logger.log(Level.FINE, "IndexTableEntry is null for: "+processedFileName+" (even if doesn't exist: "+evenIfDoesntExist+")");
			
			return null;
		}
		
		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "Using "+ite+" for: "+processedFileName);
		
		return ite.getLFN(processedFileName, evenIfDoesntExist);
	}
	

	/**
	 * @param user
	 * @param lfn
	 * @return status of the removal
	 */
	public static boolean rmLFN(final AliEnPrincipal user, final LFN lfn) {
		if (lfn!=null && lfn.exists && !lfn.isDirectory()){
			if(AuthorizationChecker.canWrite(lfn, user)){
				System.out.println("Unimplemented request from [" + user.getName() + "], rm ["
						+lfn.getCanonicalName() + "]");
				// TODO
				return false;
			} 
			return false;
			
		} 
		
		return false;
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
		
		if (lfn.perm==null)
			lfn.perm = "755";
		
		LFN parent = lfn.getParentDir(true);
		
		if (!parent.exists){
			parent.owner = lfn.owner;
			parent.gowner = lfn.gowner;
			parent.perm = lfn.perm;
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
	 * Create a new directory with a given owner
	 * 
	 * @param owner owner of the newly created structure(s)
	 * @param path the path to be created
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdir(final AliEnPrincipal owner, final String path){
		return mkdir(owner, path, false);
	}
	
	/**
	 * Create a new directory hierarchy with a given owner
	 * 
	 * @param owner owner of the newly created structure(s)
	 * @param path the path to be created
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdirs(final AliEnPrincipal owner, final String path){
		return mkdir(owner, path, true);
	}
	
	/**
	 * Create a new directory (hierarchy) with a given owner
	 * 
	 * @param owner owner of the newly created structure(s)
	 * @param path the path to be created
	 * @param createMissingParents if <code>true</code> then it will try to create any number of intermediate directories, otherwise the direct parent must already exist
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdir(final AliEnPrincipal owner, final String path, final boolean createMissingParents){
		final LFN lfn = LFNUtils.getLFN(path, true);
		
		return mkdir(owner, lfn, createMissingParents);
	}
	
	/**
	 * Create a new directory with a given owner
	 * 
	 * @param owner owner of the newly created structure(s)
	 * @param lfn the path to be created
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdir(final AliEnPrincipal owner, final LFN lfn){
		return mkdir(owner, lfn, false);
	}

	/**
	 * Create a new directory hierarchy with a given owner
	 * 
	 * @param owner owner of the newly created structure(s)
	 * @param lfn the path to be created
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdirs(final AliEnPrincipal owner, final LFN lfn){
		return mkdir(owner, lfn, true);
	}

	/**
	 * Create a new directory (hierarchy) with a given owner
	 * 
	 * @param owner owner of the newly created structure(s)
	 * @param lfn the path to be created
	 * @param createMissingParents if <code>true</code> then it will try to create any number of intermediate directories, otherwise the direct parent must already exist
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN mkdir(final AliEnPrincipal owner, final LFN lfn, final boolean createMissingParents){
		if (lfn.exists){
			if (lfn.isDirectory() && AuthorizationChecker.canWrite(lfn, owner))
				return lfn;
			
			return null;
		}
		
		lfn.owner = owner.getName();
		lfn.gowner = lfn.owner;

		lfn.size = 0;
		
		LFN parent = lfn.getParentDir(true);
		
		if (!parent.exists && !createMissingParents)
			return null;
		
		while (parent!=null && !parent.exists)
			parent = parent.getParentDir(true);
		
		if (parent!=null && parent.isDirectory() && AuthorizationChecker.canWrite(parent, owner)){
			return ensureDir(lfn);
		}
		
		return null;
	}
	
	/**
	 * @param user
	 * @param lfn
	 * @return status of the removal
	 */
	public static boolean rmdir(final AliEnPrincipal user, final LFN lfn) {
		if (lfn!=null && lfn.exists && lfn.isDirectory()){
			if(AuthorizationChecker.canWrite(lfn, user)){
				System.out.println("Unimplemented request from [" + user.getName() + "], rmdir ["
						+lfn.getCanonicalName() + "]");
				// TODO
				return false;
			}
			return false;
			
		} 
		
		return false;
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
	 * the "-y" flag of AliEn `find`
	 */
	public static final int FIND_BIGGEST_VERSION = 4;
	
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
	 * @param path
	 * @param tag
	 * @return metadata table where this tag can be found for this path, or <code>null</code> if there is no such entry
	 */
	public static String getTagTableName(final String path, final String tag){
		final DBFunctions db = ConfigUtils.getDB("alice_data");
		
		db.query("SELECT tableName FROM TAG0 WHERE tagName='"+Format.escSQL(tag)+"' AND '"+Format.escSQL(path)+"' LIKE concat(path,'%') ORDER BY length(path) DESC LIMIT 1;");
		
		if (db.moveNext())
			return db.gets(1);
		
		return null;	
	}
	
	/**
	 * @param path
	 * @param pattern
	 * @param tag
	 * @param query
	 * @param flags
	 * @return the files that match the metadata query
	 */
	public static Set<String> findByMetadata(final String path, final String pattern, final String tag, final String query, final int flags){
		final String tableName = getTagTableName(path, tag);
		
		if (tableName==null)
			return null;
		
		final DBFunctions db = ConfigUtils.getDB("alice_data");
		
		String q = "SELECT file FROM "+tableName+" WHERE file LIKE '"+Format.escSQL(path+"%"+pattern+"%")+"' AND "+Format.escSQL(query)+" ORDER BY version DESC";
		
		if ((flags & FIND_BIGGEST_VERSION) != 0)
			q += " LIMIT 1";
		
		if (!db.query(q))
			return null;
		
		final Set<String> ret = new LinkedHashSet<String>();
		
		while (db.moveNext())
			ret.add(StringFactory.get(db.gets(1)));
		
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
		
		if (lfn.exists){
			if (lfn.isCollection() && AuthorizationChecker.canWrite(lfn, owner))
				return lfn;
			
			return null;
		}
		
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
	 * @return <code>true</code> if the collection was modified
	 */
	public static boolean removeFromCollection(final LFN collection, final Set<LFN> lfns){
		if (!collection.exists || !collection.isCollection() || lfns==null || lfns.size()==0){
			return false;
		}
		
		final DBFunctions db = ConfigUtils.getDB("alice_data");
		
		db.query("SELECT collectionId FROM COLLECTIONS where collGUID=string2binary('"+collection.guid.toString()+"');");
		
		if (!db.moveNext())
			return false;
		
		final int collectionId = db.geti(1);
		
		final Set<String> currentLFNs = collection.listCollection();

		final GUID guid = GUIDUtils.getGUID(collection.guid);
		
		boolean updated = false;
		
		boolean shouldUpdateSEs = false;
		
		for (final LFN l: lfns){
			if (!currentLFNs.contains(l.getCanonicalName()))
				continue;
			
			if (!db.query("DELETE FROM COLLECTIONS_ELEM where collectionId="+collectionId+" AND origLFN='"+Format.escSQL(l.getCanonicalName())+"' AND guid=string2binary('"+l.guid.toString()+"');"))
				continue;
			
			if (db.getUpdateCount()!=1)
				continue;
			
			guid.size -= l.size;
			updated = true;
			
			if (!shouldUpdateSEs){
				final Set<PFN> whereis = l.whereisReal();
				
				if (whereis!=null){
					for (final PFN p: whereis){
						if (!guid.seStringList.contains(Integer.valueOf(p.seNumber))){
							shouldUpdateSEs = true;
							break;
						}
					}
				}
			}
		}
		
		if (updated){
			collection.size = guid.size;
			
			collection.ctime = guid.ctime = new Date();
			
			if (shouldUpdateSEs){
				Set<Integer> ses = null;
				
				final Set<String> remainingLFNs = collection.listCollection();
				
				for (final String s: remainingLFNs){
					if (ses==null || ses.size()>0){
						final LFN l = LFNUtils.getLFN(s);
						
						if (l==null)
							continue;
						
						final Set<PFN> whereis = l.whereisReal();
						
						final Set<Integer> lses = new HashSet<Integer>();
						
						for (final PFN pfn: whereis){
							lses.add(Integer.valueOf(pfn.seNumber));
						}
						
						if (ses!=null)
							ses.retainAll(ses);
						else
							ses = lses;
					}
				}
				
				if (ses!=null)
					guid.seStringList = ses;
			}
			
			guid.update();
			collection.update();
			
			return true;
		}
		
		return false;
	}
	
	/**
	 * @param collection
	 * @param lfns
	 * @return <code>true</code> if anything was changed
	 */
	public static boolean addToCollection(final LFN collection, final Set<LFN> lfns){		
		if (!collection.exists || !collection.isCollection() || lfns==null || lfns.size()==0){
			logger.log(Level.FINER, "Quick exit");
			return false;
		}
		
		final DBFunctions db = ConfigUtils.getDB("alice_data");
		
		final Set<String> currentLFNs = collection.listCollection();
		
		db.query("SELECT collectionId FROM COLLECTIONS where collGUID=string2binary('"+collection.guid.toString()+"');");
		
		if (!db.moveNext()){
			logger.log(Level.WARNING, "Didn't find any collectionId for guid " + collection.guid.toString());
			return false;
		}
		
		final int collectionId = db.geti(1);
		
		final Set<LFN> toAdd = new LinkedHashSet<LFN>();
		
		for (final LFN lfn: lfns){
			if (currentLFNs.contains(lfn.getCanonicalName()))
				continue;
			
			toAdd.add(lfn);
		}
		
		if (toAdd.size()==0){
			logger.log(Level.INFO, "Nothing to add to "+collection.getCanonicalName()+", all "+lfns.size()+" entries are listed already");
			return false;
		}
		
		final GUID guid = GUIDUtils.getGUID(collection.guid);
		
		Set<Integer> commonSEs = guid.size==0 && guid.seStringList.size()==0 ? null : new HashSet<Integer>(guid.seStringList);
			
		boolean updated = false;
		
		for (final LFN lfn: toAdd){
			if (commonSEs==null || commonSEs.size()>0){
				final Set<PFN> pfns = lfn.whereisReal();
				
				final Set<Integer> ses = new HashSet<Integer>();
		
				for (final PFN pfn: pfns){
					ses.add(Integer.valueOf(pfn.seNumber));
				}
				
				if (commonSEs!=null)
					commonSEs.retainAll(ses);
				else
					commonSEs = ses;
			}
			
			if (db.query("INSERT INTO COLLECTIONS_ELEM (collectionId,origLFN,guid) VALUES ("+collectionId+", '"+Format.escSQL(lfn.getCanonicalName())+"', string2binary('"+lfn.guid.toString()+"'));")){
				guid.size += lfn.size;
				updated = true;
			}
		}
		
		if (!updated){
			logger.log(Level.FINER, "No change to the collection");
			return false;	// nothing changed
		}
		
		if (commonSEs!=null)
			guid.seStringList = commonSEs;
		
		collection.size = guid.size;
		
		collection.ctime = guid.ctime = new Date();
		
		guid.update();
		collection.update();
		
		return true;
	}
	
}
