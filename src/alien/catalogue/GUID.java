package alien.catalogue;

import java.io.Serializable;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;

import lazyj.DBFunctions;

/**
 * @author costing
 *
 */
public class GUID implements Serializable {
	private static final long serialVersionUID = -2625119814122149207L;

	/**
	 * GUID id
	 */
	public int guidId;
	
	/**
	 * Creation time
	 */
	Date ctime;
	
	/**
	 * Username
	 */
	String owner;
	
	/**
	 * References
	 */
	int ref;
	
	/**
	 * SE IDs
	 */
	Set<Integer> seStringList;
	
	/**
	 * ?
	 */
	Set<Integer> seAutoStringList;
	
	/**
	 * ?
	 */
	int aclId;
	
	/**
	 * ?
	 */
	Date expiretime;
	
	/**
	 * File size, in bytes
	 */
	long size;
	
	/**
	 * Group name
	 */
	String gowner;
	
	/**
	 * UUID
	 */
	UUID guid;
	
	/**
	 * File type
	 */
	char type;
	
	/**
	 * MD5 checksum
	 */
	String md5;
	
	/**
	 * Permissions
	 */
	String perm;
	
	int host;
	
	int tableName;
	
	/**
	 * Load one row from a G*L table
	 * 
	 * @param db
	 * @param host 
	 * @param tableName 
	 */
	public GUID(final DBFunctions db, final int host, final int tableName){
		init(db);
		
		this.host = host;
		this.tableName = tableName;
	}
		
	private void init(final DBFunctions db){
		guidId = db.geti("guidId");
		
		ctime = db.getDate("ctime");
		
		owner = db.gets("owner");
		
		ref = db.geti("ref");
		
		seStringList = stringToSet(db.gets("seStringlist"));
		
		seAutoStringList = stringToSet(db.gets("seAutoStringlist"));
		
		aclId = db.geti("aclId", -1);
		
		expiretime = db.getDate("expiretime", null);
		
		size = db.getl("size");
		
		gowner = db.gets("gowner");
		
		final byte[] guidBytes = db.getBytes("guid");
		
		guid = getUUID(guidBytes);
		
		type = 0;
		
		String sTemp = db.gets("type");
		
		if (sTemp.length()>0)
			type = sTemp.charAt(0);
		
		md5 = db.gets("md5");
		
		perm = db.gets("perm");
	}
	
	@Override
	public String toString() {
		return "guidID\t\t: "+guidId+"\n"+
		       "ctime\t\t: "+ctime.toString()+"\n"+
		       "owner\t\t: "+owner+":"+gowner+"\n"+
		       "SE lists\t: "+seStringList+" , "+seAutoStringList+"\n"+
		       "aclId\t\t: "+aclId+"\n"+
		       "expireTime\t: "+expiretime+"\n"+
		       "size\t\t: "+size+"\n"+
		       "guid\t\t: "+guid+" ("+guid.timestamp()+")\n"+
		       "type\t\t: "+type+"\n"+
		       "md5\t\t: "+md5+"\n"+
		       "permissions\t: "+perm
		       ;
	}
	
	private static final Set<Integer> stringToSet(final String s){
		final Set<Integer> ret = new LinkedHashSet<Integer>();

		if (s==null || s.length()==0)
			return ret;		
		
		final StringTokenizer st = new StringTokenizer(s, " \t,;");
		
		while (st.hasMoreTokens()){
			try{
				ret.add(Integer.valueOf(st.nextToken()));
			}
			catch (NumberFormatException nfe){
				// ignore
			}
		}
		
		return ret;
	}
	
	/**
	 * Get the UUID for the given value array
	 *  
	 * @param data
	 * @return the UUID
	 */
	public static final UUID getUUID(final byte[] data){
        long msb = 0;
        long lsb = 0;
        assert data.length == 16;
		
        for (int i=0; i<8; i++)
            msb = (msb << 8) | (data[i] & 0xff);
        for (int i=8; i<16; i++)
            lsb = (lsb << 8) | (data[i] & 0xff);
        
        return new UUID(msb, lsb);
	}
	
}
