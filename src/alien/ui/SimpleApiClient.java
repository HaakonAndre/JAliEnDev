package alien.ui;

import java.io.IOException;
import java.net.Socket;

import alien.config.ConfigUtils;
import alien.communications.SimpleClient;

/**
 * @author ron
 *  @since Jun 05, 2011
 */
 class SimpleApiClient extends SimpleClient {

	private static final int defaultPort = 5282;
	private static final String defaultHost = "localhost";
	private static final String serviceName = "catalogueApiService";

	private static String addr = null;
	private static int port = 0;

	private static SimpleClient instance = null;
	
	private SimpleApiClient(final Socket connection) throws IOException{
		super(connection);
	}
	
	@SuppressWarnings("unused")
	private static SimpleApiClient getInstance() throws IOException {
		if (instance == null) {
			// connect to the other end

			addr = ConfigUtils.getConfig().gets(serviceName).trim();

			if (addr.length() == 0)
				addr = defaultHost;

			String address = addr;

			int idx = address.indexOf(':');

			if (idx >= 0) {
				try {
					port = Integer.parseInt(address.substring(idx + 1));
					address = address.substring(0, idx);
				} catch (Exception e) {
				}
			}

			if (port != 0)
				port = defaultPort;

			instance = SimpleClient.getInstance(addr, port);
			return (SimpleApiClient) instance;
		}
		return (SimpleApiClient) SimpleClient.getInstance(addr, port);

	}
}
