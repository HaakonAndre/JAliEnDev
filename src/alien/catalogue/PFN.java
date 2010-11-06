package alien.catalogue;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

import lazyj.DBFunctions;

/**
 * Wrapper around a G*L_PFN row
 * 
 * @author costing
 *
 */
public class PFN {
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(PFN.class.getCanonicalName());
	
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
	
	private Set<PFN> realPFNs = null;
	
	/**
	 * @param db
	 * @param host
	 * @param tableNumber
	 */
	public PFN(final DBFunctions db, final int host, final int tableNumber){
		this.host = host;
		this.tableNumber = tableNumber;
		
		init(db);
	}
	
	private void init(final DBFunctions db){
		guidId = db.geti("guidId");
		
		pfn = db.gets("pfn");
		
		seNumber = db.geti("seNumber");
	}
	
	@Override
	public String toString() {
		return "PFN: guidId\t: "+guidId+"\n"+
		       "pfn\t\t: "+pfn+"\n"+
		       "seNumber\t: "+seNumber;
	}
	
	/**
	 * @return the physical locations
	 */
	public Set<PFN> getRealPFNs(){
		if (realPFNs!=null)
			return realPFNs;
		
		if (pfn.startsWith("guid://")){
			int idx = 7;
			
			String uuid;
			
			while (pfn.charAt(idx)=='/' && idx<pfn.length()-1)
				idx++;
			
			int idx2 = pfn.indexOf('?', idx);
			
			if (idx2<0)
				uuid = pfn.substring(idx);
			else
				uuid = pfn.substring(idx, idx2);
			
			final GUID guid = GUIDUtils.getGUID(UUID.fromString(uuid));
			
			if (guid!=null)
				realPFNs = guid.getPFNs();
			else
				realPFNs = null;
		}
		else{
			realPFNs = new LinkedHashSet<PFN>(1);
			realPFNs.add(this);
		}
		
		return realPFNs;
	}
}
