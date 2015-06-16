package alien;

import alien.api.DispatchSSLClient;
import alien.site.JobAgent;
import alien.user.JAKeyStore;

/** 
 * @author mmmartin, ron
 * @since  Apr 1, 2015
 */
public class StartJobAgent {
	
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
//		try {
//			JAKeyStore.loadPilotKeyStorage();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		DispatchSSLClient.overWriteServiceAndForward("siteProxyService");
		
		JobAgent jA = new JobAgent();
		jA.start();

	}
}
