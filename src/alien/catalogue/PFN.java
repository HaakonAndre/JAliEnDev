package alien.catalogue;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.StringFactory;
import alien.catalogue.access.AccessTicket;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * Wrapper around a G*L_PFN row
 * 
 * @author costing
 *
 */
public class PFN implements Serializable, Comparable<PFN>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3854116042004576123L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(PFN.class.getCanonicalName());
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(PFN.class.getCanonicalName());
	
	/**
	 * guidID
	 */
	public int guidId;
	
	/**
	 * PFN 
	 */
	public String pfn;
	
	/**
	 * SE number
	 */
	public int seNumber;
	
	/**
	 * index
	 */
	public int host;
	
	/**
	 * table name
	 */
	public int tableNumber;
	
	/**
	 * GUID references
	 */
	private UUID uuid;
	
	/**
	 * GUID
	 * @see #getGuid()
	 */
	private GUID guid;
	
	private Set<PFN> realPFNs = null;

	/**
	 * Access ticket, if needed
	 */
	public AccessTicket ticket = null;
	
	private final int hashCode;
	
	/**
	 * @param db
	 * @param host
	 * @param tableNumber
	 */
	PFN(final DBFunctions db, final int host, final int tableNumber){
		this.host = host;
		this.tableNumber = tableNumber;
		
		init(db);
		
		hashCode = pfn.hashCode();
	}
	
	private void init(final DBFunctions db){
		guidId = db.geti("guidId");
		
		pfn = StringFactory.get(db.gets("pfn"));
		
		seNumber = db.geti("seNumber");
		
		if (seNumber>0){
			final SE se = SEUtils.getSE(seNumber);
			
			if (se!=null && se.seioDaemons!=null && se.seioDaemons.length()>0 && !pfn.startsWith(se.seioDaemons)){
				int idx = pfn.indexOf("://");
				
				if (idx>0 && idx<10){
					idx = pfn.indexOf('/', idx+3);
					
					if (idx>0)
						pfn = se.seioDaemons + pfn.substring(idx);
				}
			}
		}
	}
	
	/**
	 * Generate a new PFN
	 * 
	 * @param guid
	 * @param se
	 */
	public PFN(final GUID guid, final SE se){
		this.guid = guid;
		this.guidId = guid.guidId;
		this.pfn = se.generatePFN(guid);
		this.seNumber = se.seNumber;
		this.host = guid.host;
		this.tableNumber = guid.tableName;
		this.hashCode = this.pfn!=null ? this.pfn.hashCode() : guid.hashCode();
	}
	
	/**
	 * Load a PFN Object from String
	 * @param pfn 
	 * @param guid
	 * @param se
	 */
	public PFN(final String pfn, final GUID guid, final SE se){
		this.guid = guid;
		this.guidId = guid.guidId;
		this.pfn = pfn;
		this.seNumber = se.seNumber;
		this.host = guid.host;
		this.tableNumber = guid.tableName;
		this.hashCode = this.pfn.hashCode();
	}
	
	/**
	 * @param path correct path for case-sensitive locations
	 */
	public void setPath(final String path){
		if (this.pfn.toLowerCase().endsWith(path.toLowerCase())){
			this.pfn = this.pfn.substring(0, this.pfn.length()-path.length()) + path;
		}
	}
	
	@Override
	public String toString() {
		return "PFN: guidId\t: "+guidId+"\n"+
		       "pfn\t\t: "+pfn+"\n"+
		       "seNumber\t: "+seNumber+"\n"+
		       "GUID cache value: "+guid;
	}
	
	/**
	 * @return the PFN URL
	 */
	public String getPFN(){
		return pfn;
	}
	
	/**
	 * @return the physical locations
	 */
	public Set<PFN> getRealPFNs(){
		if (realPFNs!=null)
			return realPFNs;
		
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
				realPFNs = archiveGuid.getPFNs();
			else
				realPFNs = null;
		}
		else{
			realPFNs = new LinkedHashSet<PFN>(1);
			realPFNs.add(this);
		}
		
		return realPFNs;
	}
	
	/**
	 * Set the UUID, when known, to avoid reading from database
	 * 
	 * @param uid
	 */
	void setUUID(final UUID uid){
		uuid = uid;
	}
	
	/**
	 * Set the GUID, when known, to avoid reading from database
	 * 
	 * @param guid
	 */
	void setGUID(final GUID guid){
		this.guid = guid;
	}

	/**
	 * @return get the UUID associated to the GUID of which this entry is a replica
	 */
	public UUID getUUID(){
		if (uuid==null){
			if (guid!=null){
				uuid = guid.guid;
			}
			else{
				getGuid();
			}
		}
		
		return uuid;
	}
	
	/**
	 * @return the GUID for this PFN
	 */
	public GUID getGuid(){
		if (guid==null){
			if (uuid!=null){
				guid = GUIDUtils.getGUID(uuid);
			}
			else{
				final Host h = CatalogueUtils.getHost(host);
			
				if (h==null)
					return null;
			
				final DBFunctions db = h.getDB();
			
				if (db==null)
					return null;
			
				if (monitor!=null){
					monitor.incrementCounter("GUID_db_lookup");
				}
				
				db.query("SELECT * FROM G"+tableNumber+"L WHERE guidId="+guidId);
				
				if (db.moveNext()){
					guid = new GUID(db, host, tableNumber);
					uuid = guid.guid;
				}
			}
		}
		
		return guid;
	}
	
	@Override
	public int compareTo(final PFN o) {
		return pfn.compareTo(o.pfn);
	}
	
	@Override
	public boolean equals(final Object obj) {
		if (! (obj instanceof PFN))
			return false;
		
		return compareTo((PFN) obj) == 0;
	}
	
	@Override
	public int hashCode() {
		return this.hashCode;
	}
	
	private boolean isArchiveLinkedGUID(){
		return (pfn.toLowerCase()).startsWith("guid://");
	}

	/**
	 * @return the GUID to which this PFN points to
	 */
	public UUID retrieveArchiveLinkedGUID() {
		if(isArchiveLinkedGUID())
			return UUID.fromString(pfn.substring(8, 44));
		return null;
	}
	
}
