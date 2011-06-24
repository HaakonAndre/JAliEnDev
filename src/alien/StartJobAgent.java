package alien;

import alien.api.DispatchSSLClient;
import alien.site.JobAgent;
import alien.user.JAKeyStore;

/**
 * @author ron
 *  @since Jun 22, 2011
 */
public class StartJobAgent {
	
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		try {
			JAKeyStore.loadPilotKeyStorage();
		} catch (Exception e) {
			e.printStackTrace();
		}

		DispatchSSLClient.overWriteServiceAndForward("siteProxyService");
		
		JobAgent jA = new JobAgent();
		jA.start();

	}
}
