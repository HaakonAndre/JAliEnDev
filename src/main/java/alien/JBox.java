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

	private static void logLoud(Level l, String msg) {
		logger.log(l, msg);
		System.err.println("> " + msg); // Prepend stderr messages with ">" to make JShell print them
	}

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
				iDebug = ((Integer) options.valueOf("debug")).intValue();
		}
		catch (final Exception e) {
			// nothing, we just let it 0, nothing to debug
			e.printStackTrace();
		}

		if (!JAKeyStore.loadKeyStore()) {
			logLoud(Level.SEVERE, "ERROR: JBox failed to load any credentials");
			return;
		}

		if (!JAKeyStore.bootstrapFirstToken()) {
			logLoud(Level.SEVERE, "ERROR: JBox failed to get a token");
			return;
		}

		if (JAKeyStore.isLoaded("token") && !JAKeyStore.isLoaded("user") && !JAKeyStore.isLoaded("host")) {
			logLoud(Level.INFO, "WARNING: JBox is connected to central services with a token that cannot be used to update itself.");
			logLoud(Level.INFO, "Please use a user or host certificate to refresh tokens automatically.");
		}

		JBoxServer.startJBoxService();
		TomcatServer.startTomcatServer();

		JAKeyStore.startTokenUpdater();

		if (!ConfigUtils.writeJClientFile(ConfigUtils.exportJBoxVariables(iDebug)))
			logLoud(Level.INFO, "Failed to export JBox variables");
	}
}
