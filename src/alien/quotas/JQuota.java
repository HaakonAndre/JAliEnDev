/**
 * 
 */
package alien.quotas;

import lazyj.DBFunctions;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public class JQuota {

	/**
	 * Account name
	 */
	public final String account;
	
	/**
	 * @param db
	 */
	public JQuota(final DBFunctions db){
		account = null;
	}
}
