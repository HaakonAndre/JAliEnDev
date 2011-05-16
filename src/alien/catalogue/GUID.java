package alien.catalogue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SE;

/**
 * @author costing
 *
 */
public class GUID implements Comparable<GUID>, CatalogEntity {
	private static final long serialVersionUID = -2625119814122149207L;
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(GUID.class.getCanonicalName());
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(GUID.class.getCanonicalName());

	/**
	 * GUID id
	 */
	public int guidId;
	
	/**
	 * Creation time
	 */
	public Date ctime;
	
	/**
	 * Username
	 */
	public String owner;
	
	/**
	 * References
	 */
	public int ref;
	
	/**
	 * SE IDs
	 */
	public Set<Integer> seStringList;
	
	/**
	 * ?
	 */
	public Set<Integer> seAutoStringList;
	
	/**
	 * ?
	 */
	public int aclId;
	
	/**
	 * ?
	 */
	public Date expiretime;
	
	/**
	 * File size, in bytes
	 */
	public long size;
	
	/**
	 * Group name
	 */
	public String gowner;
	
	/**
	 * UUID
	 */
	public UUID guid;
	
	/**
	 * File type
	 */
	public char type;
	
	/**
	 * MD5 checksum
	 */
	public String md5;
	
	/**
	 * Permissions
	 */
	public String perm;
	
	/**
	 * Host where this entry was read from
	 */
	public final int host;
	
	/**
	 * Table name where this entry was read from
	 */
	public final int tableName;
	
	/**
	 * LFNs associated to this GUID
	 */
	Set<LFN> lfns;
	
	/**
	 * Set to <code>true</code> if the entry existed in the database, or to <code>false</code> if not.
	 * Setting the other fields will only be permitted if this field is false.
	 */
	private boolean exists;
	
	/**
	 * Load one row from a G*L table
	 * 
	 * @param db
	 * @param host 
	 * @param tableName 
	 */
	GUID(final DBFunctions db, final int host, final int tableName){
		init(db);
		
		this.exists = true;
		this.host = host;
		this.tableName = tableName;
	}
	
	/**
	 * Create a new GUID
	 * @param newID 
	 */
	GUID(final UUID newID){
		this.guid = newID;

		this.exists = false;
		
		this.host = GUIDUtils.getGUIDHost(guid);
		this.tableName = GUIDUtils.getTableNameForGUID(guid);
		
		seStringList = new LinkedHashSet<Integer>();
		seAutoStringList = new LinkedHashSet<Integer>();
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
	
	/**
	 * Inform the GUID about another replica in the given SE. If the entry 
	 * 
	 * @param seNumber
	 * @return true if updating was ok, false if the entry was not updated
	 */
	private boolean addSE(final int seNumber){
		final Integer i = Integer.valueOf(seNumber);
		
		if (!seStringList.contains(i)){
			seStringList.add(i);
			
			return update();
		}
		
		return false;
	}
	
	/**
	 * Inform the GUID about another replica in the given SE. If the entry 
	 * 
	 * @param seNumber
	 * @return true if updating was ok, false if the entry was not updated
	 */
	private boolean removeSE(final int seNumber){
		final Integer i = Integer.valueOf(seNumber);
		
		if (!seStringList.remove(i))
			return false;
		
		return update();
	}
	
	private boolean update(){
		final Host h = CatalogueUtils.getHost(host);
		
		if (h == null){
			return false;
		}

		final DBFunctions db = h.getDB();

		if (db == null){
			return false;
		}
			
		if (!exists){
			final boolean insertOK = insert(db);
			
			return insertOK;
		}
		
		// only the SE list can change
		if (!db.query("UPDATE G"+tableName+"L SET seStringlist="+setToString(seStringList)+" WHERE guidId="+guidId)){
			// wrong table name or what?
			return false;
		}
		
		if (db.getUpdateCount()==0){
			// the entry did not exist in fact, what's going on?
			return false;
		}

		if (monitor != null)
			monitor.incrementCounter("GUID_db_update");
		
		return true;
	}
	
	private static final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private boolean insert(final DBFunctions db){
		String q;
		
		synchronized (formatter){
			q = "INSERT INTO G"+tableName+"L (ctime, owner, ref, seStringList, seAutoStringList, aclId, expiretime, size, gowner, guid, type, md5, perm) VALUES ("+
			(ctime==null ? "null" : "'"+formatter.format(ctime)+"'")+","+		// ctime
			"'"+Format.escSQL(owner)+"',"+										// owner
			"0,"+																// ref
			setToString(seStringList)+","+										// seStringList
			setToString(seAutoStringList)+","+									// seAutoStringList
			aclId+","+															// aclId
			(expiretime==null ? "null" : "'"+formatter.format(expiretime)+"'")+","+		// expiretime
			size+","+															// size
			"'"+Format.escSQL(gowner)+"',"+										// gowner
			"string2binary('"+guid+"'),"+										// guid
			(type==0 ? "null" : "'"+type+"'")+","+								// type
			"'"+Format.escSQL(md5)+"',"+										// md5
			"'"+Format.escSQL(perm)+"'"+										// perm
			");";
		}
		
		if (db.query(q)){
			if (monitor != null)
				monitor.incrementCounter("GUID_db_insert");
			
			exists = true;
			
			db.query("SELECT guidId FROM G"+tableName+"L WHERE guid=string2binary('"+guid+"');");
			
			if (!db.moveNext()){
				// that would be weird, we have just inserted it. but double checking cannot hurt
				return false;
			}
			
			guidId = db.geti(1);
			return true;
		}
		
		return false;
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
		       "guid\t\t: "+guid+"\n"+
		       "type\t\t: "+type+" ("+(int)type+")\n"+
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
	
	private static final String setToString(final Set<Integer> s){
		if (s==null)
			return "null";
		
		if (s.size()==0)
			return "','";
		
		final StringBuilder sb = new StringBuilder("',");
		
		for (final Integer i: s){
			sb.append(i).append(',');
		}
		
		sb.append('\'');
		
		return sb.toString();
	}
	
	private Set<PFN> pfnCache = null;
	
	/**
	 * Clear the PFN cache
	 */
	public void cleanPFNCache(){
		pfnCache = null;
	}

	/**
	 * Get the PFNs for this GUID
	 * 
	 * @return set of physical locations
	 */
	public Set<PFN> getPFNs(){
		if (pfnCache!=null)
			return pfnCache;
		
		final Host h = CatalogueUtils.getHost(host);

		if (h==null)
			return null;
		
		final DBFunctions db = h.getDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("PFN_db_lookup");
		}
		
		final String q = "SELECT distinct guidId, pfn, seNumber FROM G"+tableName+"L_PFN WHERE guidId="+guidId; 

		db.query(q);
		
		pfnCache = new LinkedHashSet<PFN>();
		
		while (db.moveNext()){
			final PFN pfn = new PFN(db, host, tableName);
			
			pfn.setGUID(this);
			
			pfnCache.add(pfn);
		}
		
		return pfnCache;
	}
	
	/**
	 * Package protected. Should be accessed only from BookingTable
	 * 
	 * @param pfn
	 * @return true if inserting was ok
	 */
	boolean addPFN(final PFN pfn){
		final Host h = CatalogueUtils.getHost(host);

		if (h==null){
			return false;
		}
		
		final DBFunctions db = h.getDB();
		
		if (db==null){
			return false;
		}
		
		if (monitor!=null){
			monitor.incrementCounter("PFN_db_insert");
		}

		if (!addSE(pfn.seNumber)){
			return false;
		}
		
		if (!db.query("INSERT INTO G"+tableName+"L_PFN (guidId, pfn, seNumber) VALUES ("+guidId+", '"+Format.escSQL(pfn.getPFN())+"', "+pfn.seNumber+")")){
			seStringList.remove(Integer.valueOf(pfn.seNumber));
			update();
			return false;
		}
		
		return true;
	}
	
	/**
	 * Remove an associated PFN. It does <b>NOT</b> check if it was the last PFN.
	 * 
	 * @param pfn
	 * @return <code>true</code> if the PFN could be removed
	 */
	public boolean removePFN(final PFN pfn){
		final Host h = CatalogueUtils.getHost(host);

		if (h==null){
			return false;
		}
		
		final DBFunctions db = h.getDB();
		
		if (db==null){
			return false;
		}
		
		if (monitor!=null){
			monitor.incrementCounter("PFN_db_delete");
		}

		if (!removeSE(pfn.seNumber)){
			return false;
		}
		
		if (!db.query("DELETE FROM G"+tableName+"L_PFN WHERE guidId="+guidId+" AND pfn='"+Format.escSQL(pfn.getPFN())+"' AND seNumber="+pfn.seNumber)){
			seStringList.add(Integer.valueOf(pfn.seNumber));
			update();
			return false;
		}
		
		return true;
		
	}
	
	/**
	 * Remove the associated PFN from this particular SE 
	 * 
	 * @param se
	 * @return The PFN that was deleted, <code>null</code> if no change happened
	 */
	public String removePFN(final SE se){
		if (se==null || !seStringList.contains(Integer.valueOf(se.seNumber)))
			return null;
		
		final Set<PFN> pfns = getPFNs();
		
		if (pfns==null || pfns.size()==0)
			return null;
		
		for (final PFN pfn: pfns){
			if (pfn.seNumber == se.seNumber){
				if (removePFN(pfn))
					return pfn.pfn;
				
				break;
			}
		}
		
		return null;
	}
	
	private Set<LFN> lfnCache = null;
	
	/**
	 * Clear the cache, in case you expect the structure to have changed since the last call
	 */
	public void cleanLFNCache(){
		lfnCache = null;
	}
	
	/**
	 * @return the LFNs associated to this GUID
	 */
	public Set<LFN> getLFNs(){
		if (lfnCache!=null)
			return lfnCache;
		
		final DBFunctions db = GUIDUtils.getDBForGUID(guid);
		
		if (db==null)
			return null;
		
		final int tablename = GUIDUtils.getTableNameForGUID(guid);
		
		if (monitor!=null){
			monitor.incrementCounter("LFNREF_db_lookup");
		}
		
		db.query("SELECT distinct lfnRef FROM G"+tablename+"L_REF WHERE guidId="+guidId);
		
		if (!db.moveNext())
			return null;
		
		lfnCache = new LinkedHashSet<LFN>();
		
		do{
			final String sLFNRef = db.gets(1);
		
			final int idx = sLFNRef.indexOf('_');
		
			final int iHostID = Integer.parseInt(sLFNRef.substring(0, idx));
		
			final int iLFNTableIndex = Integer.parseInt(sLFNRef.substring(idx+1));
		
			final DBFunctions db2 = CatalogueUtils.getHost(iHostID).getDB();
			
			if (monitor!=null){
				monitor.incrementCounter("LFN_db_lookup");
			}
		
			db2.query("SELECT * FROM L"+iLFNTableIndex+"L WHERE guid=string2binary('"+guid+"');");
		
			while (db2.moveNext()){
				lfnCache.add(new LFN(db2, CatalogueUtils.getIndexTable(iHostID, iLFNTableIndex)));
			}
		}
		while (db.moveNext());
		
		return lfnCache;
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

	@Override
	public int compareTo(final GUID o) {
		return guid.compareTo(o.guid);
	}
	
	@Override
	public boolean equals(final Object obj) {
		if (! (obj instanceof GUID))
			return false;
		
		return compareTo((GUID) obj) == 0;
	}
	
	@Override
	public int hashCode() {
		return guid.hashCode();
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
		return guid.toString();
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
	
	private static final int hexToInt(final char c){
		if (c>='0' && c<='9') return c-'0';
		if (c>='a' && c<='f') return c+10-'a';
		if (c>='A' && c<='F') return c+10-'A';
		return 0;
	}
	
	/**
	 * From AliEn/GUID.pm#GetCHash
	 * 
	 * @return hash code
	 */
	public int getCHash(){
		int csum = 0;
		
		for (char c: guid.toString().toCharArray()){
			if (c!='-'){
				csum += hexToInt(c);
			}
		}
		
		return csum%16;
	}
	
	/**
	 * From AliEn/GUID.pm#GetHash
	 * 
	 * @return hash code
	 */
	public int getHash(){
		int c0 = 0;
		int c1 = 0;
		
		for (char c: guid.toString().toCharArray()){
			if (c!='-'){
				c0 += hexToInt(c);
				c1 += c0;
			}
		}
		
		c0 &= 0xFF;
		c1 &= 0xFF;

		int x = c1 % 255;
		int y = (c1 - c0) % 255;
		
		if (y<0)
			y = 255 + y;
		
		return (y<<8) + x;
	}
	
	/**
	 * @return <code>true</code> if the guid was taken from the database, <code>false</code> if it is a newly generated one
	 */
	public boolean exists(){
		return exists;
	}
	
	/**
	 * @return the set of real GUIDs of this file
	 */
	public Set<GUID> getRealGUIDs(){
		if (!exists)
			return null;
		
		final Set<GUID> ret = new HashSet<GUID>();
		
		final Set<PFN> pfns = getPFNs();
		
		for (final PFN replica: pfns){
			final String pfn = replica.pfn;
			
			if (pfn.startsWith("guid://")){
				int idx = 7;
				
				String sUuid;
				
				while (pfn.charAt(idx)=='/' && idx<pfn.length()-1)
					idx++;
				
				int idx2 = pfn.indexOf('?', idx);
				
				if (idx2<0)
					sUuid = pfn.substring(idx);
				else
					sUuid = pfn.substring(idx, idx2);
				
				final GUID archiveGuid = GUIDUtils.getGUID(UUID.fromString(sUuid));
				
				if (archiveGuid!=null)
					ret.add(archiveGuid);
			}
		}
		
		if (ret.size()==0)
			ret.add(this);
		
		return ret;
	}
}
