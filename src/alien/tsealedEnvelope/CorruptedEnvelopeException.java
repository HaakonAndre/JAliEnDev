package alien.tsealedEnvelope;

public class CorruptedEnvelopeException extends Exception {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public CorruptedEnvelopeException(String message){
		System.out.println(message);
	}
}
