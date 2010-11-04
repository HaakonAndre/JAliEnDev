/**
 * 
 */
package alien.quotas;

import lazyj.DBFunctions;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public class FQuota {

	/**
	 * Account name
	 */
	public final String account;
	
	/**
	 * @param db
	 */
	public FQuota(final DBFunctions db){
		account = null;
	}
	
}
