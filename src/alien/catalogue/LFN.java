package alien.catalogue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.config.ConfigUtils;

/**
 * @author costing
 *
 */
public class LFN implements Comparable<LFN>, CatalogEntity {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4419468872696081193L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(LFN.class.getCanonicalName());
	
	/**
	 * entryId
	 */
	public long entryId;
	
	/**
	 * Owner
	 */
	public String owner;
	
	/**
	 * Last change timestamp
	 */
	public Date ctime;
	
	/**
	 * If more than one copy
	 */
	public boolean replicated;
	
	/**
	 * ACL id
	 */
	public int aclId;
	
	/**
	 * short LFN
	 * @see IndexTableEntry
	 */
	public String lfn;
	
	/**
	 * Expiration time
	 */
	public Date expiretime;
	
	/**
	 * Size, in bytes
	 */
	public long size;
	
	/**
	 * Parent directory, in the same IndexTableEntry
	 */
	public long dir;
	
	/**
	 * Group
	 */
	public String gowner;
	
	/**
	 * File type
	 */
	public char type;
	
	/**
	 * Access rights
	 */
	public String perm;
	
	/**
	 * SE list
	 */
	public long selist;
	
	/**
	 * The unique identifier
	 */
	public UUID guid;
	
	/**
	 * MD5 checksum
	 */
	public String md5;
	
	/**
	 * GUID time (in GUIDINDEX short style)
	 */
	public String guidtime;
	
	/**
	 * ?
	 */
	public boolean broken;
	
	/**
	 * Whether or not this entry really exists in the catalogue
	 */
	public boolean exists = false;
	
	/**
	 * Parent directory
	 */
	public LFN parentDir = null; 
	
	/**
	 * Canonical path
	 */
	private String canonicalName = null;
	
	/**
	 * The table where this row can be found
	 */
	public IndexTableEntry indexTableEntry;
	
	/**
	 * Job ID that produced this file
	 * @since AliEn 2.19
	 */
	public int jobid;
	
	/**
	 * @param lfn
	 * @param entry
	 */
	LFN(final String lfn, final IndexTableEntry entry){
		this.lfn = lfn;
		this.indexTableEntry = entry;
		
		int idx = lfn.lastIndexOf('/');
		
		if (idx==lfn.length()-1)
			idx = lfn.lastIndexOf('/', idx);
		
		if (idx>=0){
			final String sDir = lfn.substring(0, idx-1);
			
			parentDir = LFNUtils.getLFN(sDir, true);
			
			if (parentDir!=null){
				dir = parentDir.entryId;
				
				owner = parentDir.owner;
				
				gowner = parentDir.gowner;
				
				perm = parentDir.perm;
			}
		}
	}
	
	/**
	 * Get the parent directory
	 * 
	 * @return parent directory
	 */
	public LFN getParentDir(){
		return getParentDir(false);
	}
	
	/**
	 * @param evenIfNotExist
	 * @return parent directory
	 */
	LFN getParentDir(final boolean evenIfNotExist){
		if (parentDir!=null)
			return parentDir;
		
		if (dir>0)
			parentDir = indexTableEntry.getLFN(dir);
		
		if (parentDir==null){
			String sParentDir = getCanonicalName();
			
			if (sParentDir.length()>1){
				int idx = sParentDir.lastIndexOf('/');
				
				if (idx==sParentDir.length()-1)
					idx = sParentDir.lastIndexOf('/', idx-1);
				
				if (idx>=0)
					parentDir = LFNUtils.getLFN(sParentDir.substring(0, idx+1), evenIfNotExist);
			}
		}
		
		return parentDir;
	}
	
	/**
	 * @param db
	 * @param entry 
	 */
	public LFN(final DBFunctions db, final IndexTableEntry entry){
		init(db);
		
		this.indexTableEntry = entry;
	}
	
	@Override
	public int hashCode() {
		return (int)dir*13 + lfn.hashCode()*17;
	}
	
	private void init(final DBFunctions db){
		exists = true;
		
		entryId = db.getl("entryId");
		
		owner = db.gets("owner");
		
		ctime = db.getDate("ctime", null);
		
		replicated = db.getb("replicated", false);
		
		aclId = db.geti("aclId", -1);
		
		lfn = db.gets("lfn");
		
		expiretime = db.getDate("expiretime", null);
		
		size = db.getl("size");
		
		dir = db.geti("dir");
		
		gowner = db.gets("gowner");
		
		type = db.gets("type").charAt(0);
		
		perm = db.gets("perm");
		
		selist = db.getl("selist");

		byte[] guidBytes = db.getBytes("guid");
		
		if (guidBytes!=null)
			guid = GUID.getUUID(guidBytes);
		else
			guid = null;
		
		md5 = db.gets("md5");
		
		guidtime = db.gets("guidtime");
		
		broken = db.getb("broken", false);
		
		jobid = db.geti("jobid", -1);
	}
	
	@Override
	public String toString() {
		return "LFN entryId\t: "+entryId+"\n"+
		       "owner\t\t: "+owner+":"+gowner+"\n"+
		       "ctime\t\t: "+ctime+" (expires "+expiretime+")\n"+
		       "replicated\t: "+replicated+"\n"+
		       "aclId\t\t: "+aclId+"\n"+
		       "lfn\t\t: "+lfn+"\n"+
		       "dir\t\t: "+dir+"\n"+
		       "size\t\t: "+size+"\n"+
		       "type\t\t: "+type+"\n"+
		       "perm\t\t: "+perm+"\n"+
		       "selist\t\t: "+selist+"\n"+
		       "guid\t\t: "+guid+"\n"+
		       "md5\t\t: "+md5+"\n"+
		       "guidtime\t: "+guidtime+"\n"+
		       "broken\t\t: "+broken+"\n"+
		       "jobid\t\t: "+jobid
		       ;
	}
	
	/**
	 * Get the canonical name (full path and name)
	 * 
	 * @return canonical name
	 */
	public String getCanonicalName(){
		if (canonicalName!=null)
			return canonicalName;
		
		final String sLFN = indexTableEntry.lfn;
		
		final boolean bEnds = sLFN.endsWith("/");
		final boolean bStarts = lfn.startsWith("/");
		
		if (bEnds && bStarts)
			canonicalName = sLFN.substring(0, sLFN.length()-1) + lfn;
		else
		if (!bEnds && !bStarts)
			canonicalName = sLFN + "/" + lfn;
		else	
			canonicalName = sLFN + lfn;
		
		return canonicalName;
	}
	
	/**
	 * Get the physical locations of this file
	 * 
	 * @return the physical locations for this file
	 */
	public Set<PFN> whereis(){
		if (!exists || guid==null)
			return null;
		
		final GUID id = GUIDUtils.getGUID(guid);
		
		if (id==null)
			return null;
		
		return id.getPFNs();
	}
	
	/**
	 * Get the real physical locations of this file
	 * 
	 * @return real locations
	 */
	public Set<PFN> whereisReal(){
		if (!exists || guid==null)
			return null;
		
		final GUID id = GUIDUtils.getGUID(guid);
		
		if (id==null)
			return null;
		
		final Set<PFN> pfns = id.getPFNs();
		
		if (pfns==null || pfns.size()==0)
			return pfns;
		
		final Set<PFN> ret = new LinkedHashSet<PFN>(pfns.size());
		
		for (PFN p: pfns){
			final Set<PFN> real = p.getRealPFNs();
			
			if (real!=null)
				ret.addAll(real);
		}
		
		return ret;
	}

	@Override
	public int compareTo(final LFN o) {
		int diff = indexTableEntry.compareTo(o.indexTableEntry);
		
		if (diff!=0)
			return diff;
		
		return lfn.compareTo(o.lfn);
	}
	
	@Override
	public boolean equals(final Object obj) {
		if (! (obj instanceof LFN))
			return false;
		
		return compareTo((LFN) obj)==0;
	}

	/* (non-Javadoc)
	 * @see alien.catalogue.CatalogEntity#getGroup()
	 */
	@Override
	public String getGroup() {
		return gowner;
	}

	/* (non-Javadoc)
	 * @see alien.catalogue.CatalogEntity#getName()
	 */
	@Override
	public String getName() {
		return getCanonicalName();
	}

	/* (non-Javadoc)
	 * @see alien.catalogue.CatalogEntity#getOwner()
	 */
	@Override
	public String getOwner() {
		return owner;
	}

	/* (non-Javadoc)
	 * @see alien.catalogue.CatalogEntity#getPermissions()
	 */
	@Override
	public String getPermissions() {
		return perm;
	}
	
	/* (non-Javadoc)
	 * @see alien.catalogue.CatalogEntity#getType()
	 */
	@Override
	public char getType() {
		return type;
	}
	
	private static final String e(final String s){
		if (s==null)
			return "null";
		
		return "'"+Format.escSQL(s)+"'";
	}
	
	private static final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static final synchronized String format(final Date d){
		if (d==null)
			return null;
		
		return formatter.format(d);
	}
	
	/**
	 * Insert a new LFN in the catalogue
	 * 
	 * @return <code>true</code> if the new entry was inserted, <code>false</code> if the query failed
	 */
	boolean insert(){
		String q = "INSERT INTO L"+indexTableEntry.tableName+"L (owner, ctime, replicated, aclId, lfn, expiretime, size, "+
		"dir, gowner, type, perm, selist, guid, md5, guidtime, broken, jobid) VALUES ("+
		e(owner)+","+
		e(format(ctime))+","+
		(replicated ? "1" : "0")+","+
		(aclId>0 ? ""+aclId : "null")+","+
		e(lfn)+","+
		e(format(expiretime))+","+
		size+","+
		dir+","+
		e(gowner)+","+
		(type>0 ? e(""+type) : "null")+","+
		e(perm)+","+
		selist+","+
		"string2binary('"+guid+"'),"+
		e(md5)+","+
		e(guidtime)+","+
		(broken ? 1 : 0)+","+
		(jobid>0 ? ""+jobid : "null")+
		");";
		
		DBFunctions db = indexTableEntry.getDB();
		
		return db.query(q);
	}
	
	/**
	 * @return the list of entries in this folder
	 */
	public List<LFN> list(){
		if (indexTableEntry==null)
			return null;
		
		final List<LFN> ret = new ArrayList<LFN>();

		if (CatalogueUtils.isSeparateTable(getCanonicalName())){
			final IndexTableEntry other = CatalogueUtils.getClosestMatch(getCanonicalName());
			
			final DBFunctions db = other.getDB();
			
			final String q = "SELECT * FROM L"+other.tableName+"L WHERE dir=(SELECT entryId FROM L"+other.tableName+"L WHERE lfn='') AND lfn IS NOT NULL AND lfn!='' ORDER BY lfn ASC;";
						
			db.query(q);

			while (db.moveNext()){
				ret.add(new LFN(db, other));
			}
			
			return ret;
		}
		
		final DBFunctions db = indexTableEntry.getDB();
		
		final String q = "SELECT * FROM L"+indexTableEntry.tableName+"L WHERE dir="+entryId+" AND lfn IS NOT NULL AND lfn!='' ORDER BY lfn ASC;";
		
		db.query(q);
		
		while (db.moveNext()){
			ret.add(new LFN(db, indexTableEntry));
		}
				
		return ret;
	}
}
