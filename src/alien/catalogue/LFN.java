package alien.catalogue;

import java.util.Date;
import java.util.UUID;


import lazyj.DBFunctions;

/**
 * @author costing
 *
 */
public class LFN {

	long entryId;
	
	String owner;
	
	Date ctime;
	
	boolean replicated;
	
	int aclId;
	
	String lfn;
	
	Date expiretime;
	
	long size;
	
	long dir;
	
	String gowner;
	
	char type;
	
	String perm;
	
	long selist;
	
	UUID guid;
	
	String md5;
	
	String guidtime;
	
	boolean broken;
	
	int host;
	
	int tableName;
	
	boolean exists = false;
	
	LFN parentDir = null; 
	
	String canonicalName = null;
	
	LFN(final String lfn, final int host, final int tableName){
		this.lfn = lfn;
		this.host = host;
		this.tableName = tableName;
		
		int idx = lfn.lastIndexOf('/');
		
		if (idx>=0){
			String sDir = lfn.substring(0, idx);
			
			parentDir = LFNUtils.getLFN(sDir, false);
			
			if (parentDir!=null){
				dir = parentDir.entryId;
			}
		}
	}
	
	/**
	 * Get the parent directory
	 * 
	 * @return parent directory
	 */
	public LFN getParentDir(){
		if (parentDir!=null)
			return parentDir;
		
		final IndexTableEntry ite = CatalogueUtils.getIndexTable(tableName);
		
		parentDir = ite.getLFN(dir);
		
		return parentDir;
	}
	
	/**
	 * @param db
	 * @param host 
	 * @param tableName 
	 */
	public LFN(final DBFunctions db, final int host, final int tableName){
		init(db);
		
		this.host = host;
		this.tableName = tableName;
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
		
		guid = GUID.getUUID(db.getBytes("guid"));
		
		md5 = db.gets("md5");
		
		guidtime = db.gets("guidtime");
		
		broken = db.getb("broken", false);
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
		       "broken\t\t: "+broken;
	}
	
	/**
	 * Get the canonical name (full path and name)
	 * 
	 * @return canonical name
	 */
	public String getCanonicalName(){
		if (canonicalName!=null)
			return canonicalName;
		
		final IndexTableEntry entry = CatalogueUtils.getIndexTable(tableName);
		
		if (entry==null){
			return lfn;
		}
		
		final String sLFN = entry.lfn;
		
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
}
