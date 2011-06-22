package alien;

import java.security.KeyStoreException;

import alien.api.APIServer;

/**
 * @author ron
 * @since Jun 21, 2011
 */
public class APIService {

	/**
	 * Debugging method
	 * 
	 * @param args
	 * @throws KeyStoreException 
	 */
	public static void main(String[] args) throws KeyStoreException {

		APIServer.startAPIService();
	}
	
}
