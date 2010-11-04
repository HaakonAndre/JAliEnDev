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
	
	int dir;
	
	String gowner;
	
	char type;
	
	String perm;
	
	long selist;
	
	UUID guid;
	
	String md5;
	
	String guidtime;
	
	boolean broken;
	
	/**
	 * @param db
	 */
	public LFN(final DBFunctions db){
		init(db);
	}
	
	@Override
	public int hashCode() {
		return dir*13 + lfn.hashCode()*17;
	}
	
	private void init(final DBFunctions db){
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
	
}
