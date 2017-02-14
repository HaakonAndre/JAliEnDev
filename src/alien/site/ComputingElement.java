package alien.site;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import alien.api.DispatchSSLServer;
import alien.config.ConfigUtils;
import alien.user.JAKeyStore;
import lazyj.ExtProperties;

public class ComputingElement extends Thread {

	// Logger object
	static transient final Logger logger = ConfigUtils.getLogger(ComputingElement.class.getCanonicalName());
	
	// Config
	private final ExtProperties config = ConfigUtils.getConfig();
	
	private final Map<String, String> env = System.getenv();
	private HashMap<String, Object> siteMap = null;

	public ComputingElement() {
		try {
			JAKeyStore.loadClientKeyStorage();

			JAKeyStore.loadServerKeyStorage();
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {

		DispatchSSLServer.overWriteServiceAndForward("siteProxyService");

		try {
			DispatchSSLServer.runService();
		} catch (final IOException e1) {
			e1.printStackTrace();
		}

		siteMap = (new SiteMap()).getSiteParameters();

		// while (true) {
		// here we would have to poll the queue info and submit jobAgents....

		// }

	}
	
}
