package alien.shell;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.ui.Request;

/**
 * @author costing
 *
 */
public class SimpleServer extends Thread {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(SimpleServer.class.getCanonicalName());
	
	/**
	 * The entire connection
	 */
	private final Socket connection;
	
	/**
	 * Getting requests by this stream
	 */
	private final ObjectInputStream ois;
	
	/**
	 * Writing replies here
	 */
	private final ObjectOutputStream oos;
	
	private final OutputStream os;
	
	private SimpleServer(final Socket connection) throws IOException {
		this.connection = connection;
		
		connection.setTcpNoDelay(true);
		connection.setTrafficClass(0x10);
	
		this.os = connection.getOutputStream();
		
		this.oos = new ObjectOutputStream(this.os);
		this.oos.flush();
		this.os.flush();
		
		this.ois = new ObjectInputStream(connection.getInputStream());
		
		setName(connection.getInetAddress().toString());
		setDaemon(true);
	}
	
	@Override
	public void run() {
		long lLasted = 0;
		
		long lSerialization = 0;
		
		try{
			while (true){
				final Object o = ois.readObject();
				
				if (o!=null){					
					if (o instanceof Request){
						final Request r = (Request) o;

						
						long lStart = System.currentTimeMillis();
						
						r.run();
						
						lLasted += (System.currentTimeMillis() - lStart);
						
						long lSer = System.currentTimeMillis();
						
						oos.writeObject(r);
						oos.flush();
						os.flush();
						
						lSerialization += System.currentTimeMillis() - lSer;
						
						System.out.println("Got request from "+r.getRequesterIdentity()); // + ": "+r.toString());
						
						
					}
					else{
						logger.log(Level.WARNING, "I don't know what to do with an object of type "+o.getClass().getCanonicalName());
					}
				}
			}
		}
		catch (Exception e){
			System.err.println("Lasted : "+lLasted+", serialization : "+lSerialization);
			
			if (ois!=null){
				try{
					ois.close();
				}
				catch (IOException ioe){
					// ignore
				}
			}
			
			if (oos!=null){
				try{
					oos.close();
				}
				catch (IOException ioe){
					// ignore
				}
			}
			
			if (connection!=null){
				try{
					connection.close();
				}
				catch (IOException ioe){
					// ignore
				}
			}
		}
	}
	
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException{
		final ServerSocket ss = new ServerSocket(5282);
		
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
