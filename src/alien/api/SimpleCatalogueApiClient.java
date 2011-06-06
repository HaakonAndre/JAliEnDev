package alien.api;

import java.io.IOException;
import java.net.Socket;

import alien.config.ConfigUtils;
import alien.communications.SimpleClient;

/**
 * @author ron
 * @since Jun 05, 2011
 */
class SimpleCatalogueApiClient extends SimpleClient {

	private static final int defaultPort = 5282;
	private static final String defaultHost = "localhost";
	private static final String serviceName = "catalogueApiService";

	private static String addr = null;
	private static int port = 0;

	private SimpleCatalogueApiClient(final Socket connection) throws IOException {
		super(connection);
	}

	/**
	 * @param r
	 * @return the processed request, if successful
	 * @throws IOException
	 *             in case of connectivity problems
	 */
	public static synchronized Request dispatchRequest(final Request r)
			throws IOException {
		addr = ConfigUtils.getConfig().gets(serviceName).trim();

		if (addr.length() == 0) {
			addr = defaultHost;
		} else {

			String address = addr;

			int idx = address.indexOf(':');

			if (idx >= 0) {
				try {
					port = Integer.parseInt(address.substring(idx + 1));
					address = address.substring(0, idx);
				} catch (Exception e) {
				}
			}
			if (port == 0)
				port = defaultPort;

		}
		return SimpleClient.dispatchRequest(r, addr, port);
	}
}
