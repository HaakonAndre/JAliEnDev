package alien.communications;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bouncycastle.openssl.PEMReader;

import alien.api.Authenticate;
import alien.api.Request;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthenticationChecker;
import alien.user.UserFactory;

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
	
	private Authenticate authN = null;
	private AliEnPrincipal user = null;
	
	/**
	 * @param connection
	 * @throws IOException
	 */
	public SimpleServer(final Socket connection) throws IOException {
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
						
						r.user = user;
						r.run();
						
						lLasted += (System.currentTimeMillis() - lStart);
						
						long lSer = System.currentTimeMillis();
						
						oos.writeObject(r);
						oos.flush();
						os.flush();
						
						lSerialization += System.currentTimeMillis() - lSer;
						
						System.out.println("Got request from "+r.getRequesterIdentity()+" : "+r.getClass().getCanonicalName()); // + ": "+r.toString());
						
						
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
	 * @param port
	 * @throws IOException
	 */
	public static void runService(int port) throws IOException{
		
		final ServerSocket ss = new ServerSocket(port);
		
		logger.log(Level.INFO, "Server listening on "+ss.getLocalPort());
		
		while (true){
			try{
				final Socket s = ss.accept();
			
				logger.log(Level.INFO, "Got a connection from : "+s.getInetAddress());
				
				SimpleServer serv = new SimpleServer(s);
				System.out.println("Initiating Challenge, Response...");
				if(serv.authenticate()){
					System.out.println("Successfully authenticated. Have fun.");
					
					serv.user = UserFactory
							.getByCertificate(new X509Certificate[] { ((X509Certificate) new PEMReader(new StringReader(serv.authN.getPubCert()))
							.readObject()) });
					serv.start();
				} else
					System.out.println("Authentication failed.");
				
			}
			catch (IOException ioe){
				logger.log(Level.WARNING, "Exception treating a client", ioe);
			}
		}
	}
	


	/**
	 * Total amount of time (in milliseconds) spent in writing objects to the socket.
	 */
	public static long lSerialization = 0;
	
	/**
	 * @param authN 
	 * @return 
	 * @throws IOException 
	 * 
	 */
	public
	synchronized boolean authenticate() throws IOException {

		
		AuthenticationChecker authCh = new AuthenticationChecker(); 
		
		authN = new Authenticate(authCh.challenge());

			long lStart = System.currentTimeMillis();
			
			this.oos.writeObject(authN);
			//c.oos.reset();		
			this.oos.flush();
			this.os.flush();
			
			lSerialization += System.currentTimeMillis() - lStart;
			
			Authenticate authNResp;
			try {
				authNResp = (Authenticate) this.ois.readObject();
			}
			catch (ClassNotFoundException e) {
				throw new IOException(e.getMessage());
			}
			
			try {
				return authCh.verify(authNResp.getPubCert(), authNResp.getResponse());
			} catch (InvalidKeyException e) {
				e.printStackTrace();
			} catch (SignatureException e) {
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			return false;
	}
	
}
