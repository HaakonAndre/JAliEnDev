package alien.site;

import java.io.IOException;

import alien.api.DispatchSSLServer;
import alien.user.JAKeyStore;

/**
 * @author ron
 * @since Jun 05, 2011
 */
public class ComputingElement extends Thread  {

	/**
	 * 
	 */
	
	
	/**
	 * 
	 */
	public ComputingElement() {
		try {
			JAKeyStore.loadClientKeyStorage();

			JAKeyStore.loadServerKeyStorage();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public void run(){

		DispatchSSLServer.overWriteServiceAndForward("siteProxyService");
		
		try {
			DispatchSSLServer.runService();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		
	//	while (true) {
		// here we would have to poll the queue info and submit jobAgents....

	//	}

	}
}
