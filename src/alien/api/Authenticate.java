package alien.api;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;

import alien.user.AuthenticationChecker;


/**
 * Authenticate a client with challenge/response on his private key possesion
 * @author ron
 * @since Jun 17, 2011
 */
public class Authenticate extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3521063493699779404L;
	private final String challenge;
	private String response;
	private String pubCert;

	/**
	 * @param challenge 
	 * @param evenIfDoesNotExist
	 */
	public Authenticate(final String challenge) {
		this.challenge = challenge;
	}

	@Override
	public void run() {
		AuthenticationChecker authN = new AuthenticationChecker();
		try {
			response = authN.response(challenge);
			pubCert = AuthenticationChecker.readPubCert();

		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (SignatureException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return the client response
	 */
	public String getResponse() {
		return response;
	}

	/**
	 * @return the public Cert
	 */
	public String getPubCert() {
		return this.pubCert;
	}

	
	
	@Override
	public String toString() {
		return "Asked for : " + this.challenge + ", reply is: " + this.response;
	}
}
