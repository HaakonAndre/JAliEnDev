/**
 * 
 */
package alien.io.protocols;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public final class Factory {

	/**
	 * Normal (xrdcp) transfers
	 */
	public static final Xrootd xrootd = new Xrootd();
	
	/**
	 * Third-party xrootd transfers
	 */
	public static final Xrd3cp xrd3cp = new Xrd3cp();
	
	/**
	 * HTTP protocol
	 */
	public static final Http http = new Http();

	/**
	 * Torrent protocol
	 */
	public static final Torrent torrent = new Torrent();
}
