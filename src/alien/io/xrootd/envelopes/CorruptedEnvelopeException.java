package alien.io.xrootd.envelopes;

/**
 * @author Steffen
 * @since Nov 9, 2010
 */
public class CorruptedEnvelopeException extends Exception {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @param message
	 */
	public CorruptedEnvelopeException(String message){
		System.out.println(message);
	}
}
