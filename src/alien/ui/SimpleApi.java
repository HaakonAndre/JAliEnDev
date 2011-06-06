package alien.ui;

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
public class SimpleApi extends Thread {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(SimpleApi.class.getCanonicalName());
	

	private static final int defaultPort = 5282;
	private static final String servicePort = "catalogueApiService";

	
	
	/**
	 * @param args
	 */
	public void run(){
		
		int port = 0;
		try{
		port = Integer.parseInt(ConfigUtils.getConfig().gets(servicePort).trim());
		} catch(NumberFormatException e){}
		
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
