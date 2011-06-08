package alien.api;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.communications.SimpleServer;
import alien.config.ConfigUtils;

/**
 * @author ron
 *  @since Jun 05, 2011
 */
public class SimpleTaskQueueApiService extends Thread {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(SimpleTaskQueueApiService.class.getCanonicalName());
	

	private static final int defaultPort = 5283;
	private static final String serviceName = "taskQueueApiService";
	
	
	/**
	 * @param args
	 * @throws IOException
	 */
	public void run(){
		
		int port = 0;
		
		String address = ConfigUtils.getConfig().gets(serviceName).trim();

		if (address.length() != 0) {

			int idx = address.indexOf(':');

			if (idx >= 0) {
				try {
					port = Integer.parseInt(address.substring(idx + 1));
					address = address.substring(0, idx);
				} catch (Exception e) {
				}
			}
		}
		
		if(port==0)
			port = defaultPort;
		
		ServerSocket ss = null;
		try {
			ss = new ServerSocket(port);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Listening now on " + port);

		
		logger.log(Level.INFO, "Server listening on "+ss.getLocalPort());
		
		while (true){
			try{
				final Socket s = ss.accept();
			
				logger.log(Level.INFO, "Got a connection from : "+s.getInetAddress());
				
				new SimpleServer(s).start();
			}
			catch (IOException ioe){
				logger.log(Level.WARNING, "Exception treating a client", ioe);
			}
		}
	}
	
}
