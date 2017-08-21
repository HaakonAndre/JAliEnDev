package alien;

import java.security.KeyStoreException;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.JBoxServer;
import alien.api.TomcatServer;
import alien.config.ConfigUtils;
import alien.user.JAKeyStore;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since Jun 21, 2011
 */
public class JBox {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JBoxServer.class.getCanonicalName());

	/**
	 * Debugging method
	 *
	 * @param args
	 * @throws KeyStoreException
	 */
	public static void main(final String[] args) throws KeyStoreException {

		System.out.println("Starting JBox");

		logger.log(Level.FINE, "Starting JBox");

		final OptionParser parser = new OptionParser();
		parser.accepts("login");
		parser.accepts("debug").withRequiredArg().ofType(Integer.class);

		int iDebug = 0;

		try {
			final OptionSet options = parser.parse(args);

			if (options.has("debug"))
				// iDebug = Integer.parseInt((String) options.valueOf("debug"));
				iDebug = ((Integer) options.valueOf("debug")).intValue();

		} catch (final Exception e) {
			// nothing, we just let it 0, nothing to debug
			e.printStackTrace();
		}

		// First, load user certificate and create keystore
		while (true)
			try {
				if (!JAKeyStore.loadClientKeyStorage()) {
					System.err.println("Grid Certificate could not be loaded.");
					System.err.println("Exiting...");
					return;
				}
				break;
			} catch (final org.bouncycastle.openssl.EncryptionException | javax.crypto.BadPaddingException e) {
				logger.log(Level.SEVERE, "Wrong password! Try again", e);
				System.err.println("Wrong password! Try again");
			} catch (final Exception e) {
				logger.log(Level.SEVERE, "Error loading the key", e);
				System.err.println("Error loading the key");
			}

		JBoxServer.startJBoxService(iDebug);
		TomcatServer.startTomcatServer(iDebug);
	}
}