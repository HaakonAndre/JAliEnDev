package alien.perl.commands;

/**
 * @author ron
 *
 */
public class PerlSecurityException extends Exception {

	private String message;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	/**
	 * @param message
	 */
	public PerlSecurityException(String message) {
		this.message = message;
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public String getMessage() {
		return message;
	}
}
