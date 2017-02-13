package alien.site;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import alien.api.DispatchSSLServer;
import alien.user.JAKeyStore;

public class ComputingElement extends Thread {

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
