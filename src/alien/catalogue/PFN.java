package alien.catalogue;

import lazyj.DBFunctions;

/**
 * Wrapper around a G*L_PFN row
 * 
 * @author costing
 *
 */
public class PFN {

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
	
}
