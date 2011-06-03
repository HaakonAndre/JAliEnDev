package alien.perl.commands;

public class PerlSecurityException extends Exception {

	private String message;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	public PerlSecurityException(String message) {
		this.message = message;
		// TODO Auto-generated constructor stub
	}
	
	public String getMessage() {
		return message;
	}
}
