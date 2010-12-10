package alien.catalogue.access;


/**
 * @author costing
 *
 */
public class SignedEnvelope extends AccessTicket {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1024787790575833398L;

	/**
	 * Signed envelope
	 */
	public final String signedEnvelope; 
	
	/**
	 * Encrypted envelope
	 */
	public final String encryptedEnvelope;
	
	/**
	 * @param type
	 * @param signedEnvelope 
	 * @param encryptedEnvelope 
	 */
	public SignedEnvelope(final AccessType type, final String signedEnvelope, final String encryptedEnvelope){
		super(type);
		
		this.signedEnvelope = signedEnvelope;
		this.encryptedEnvelope = encryptedEnvelope;
	}
	
}
