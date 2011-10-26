package alien.api;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.SecureRandom;
import java.security.Security;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.security.cert.X509Certificate;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import alien.config.ConfigUtils;
import alien.user.JAKeyStore;

/**
 * @author costing
 * 
 */
public class DispatchSSLClient extends Thread {
	
	/**
	 * Reset the object stream every this many objects sent
	 */
	private static final int RESET_OBJECT_STREAM_COUNTER = 1000;
	
	private int objectsSentCounter = 0;
	
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(DispatchSSLClient.class
			.getCanonicalName());
	
	private static final int defaultPort = 5282;
	private static final String defaultHost = "localhost";
	private static String serviceName = "apiService";

	private static String addr = null;
	private static int port = 0;

	private final Socket connection;

	private final ObjectInputStream ois;
	private final ObjectOutputStream oos;

	private final OutputStream os;

	
	/**
	 * E.g. the CE proxy should act as a fowarding bridge between JA and central services
	 * @param servName name of the config parameter for the host:port settings
	 */
	public static void overWriteServiceAndForward(String servName){
		//TODO: we could drop the serviceName overwrite, once we assume to run not on one single host everything
		serviceName = servName;
	}
	
	
	/**
	 * @param connection
	 * @throws IOException
	 */
	protected DispatchSSLClient(final Socket connection) throws IOException {

		this.connection = connection;

		connection.setTcpNoDelay(true);
		connection.setTrafficClass(0x10);

		this.ois = new ObjectInputStream(connection.getInputStream());

		this.os = connection.getOutputStream();

		this.oos = new ObjectOutputStream(this.os);
		this.oos.flush();
	}

	@Override
	public String toString() {
		return this.connection.getInetAddress().toString();
	}

	@Override
	public void run() {
		// check
	}

	private static HashMap<Integer, DispatchSSLClient> instance = 
		new HashMap<Integer, DispatchSSLClient>(20);

	/**
	 * @param address
	 * @param p
	 * @return instance
	 * @throws IOException
	 */
	public static DispatchSSLClient getInstance(final String address, int p) throws IOException {

		if (instance.get(Integer.valueOf(p)) == null) {
			// connect to the other end
			logger.log(Level.INFO,"Connecting to " + address + ":" + p);

			Security.addProvider(new BouncyCastleProvider());

			try {

				// get factory
				KeyManagerFactory kmf = KeyManagerFactory.getInstance(
						"SunX509", "SunJSSE");

				logger.log(Level.INFO,"Connecting with client cert: "
								+ ((java.security.cert.X509Certificate) JAKeyStore.clientCert
										.getCertificateChain("User.cert")[0])
										.getSubjectDN());

				// initialize factory, with clientCert(incl. priv+pub)
				kmf.init(JAKeyStore.clientCert, JAKeyStore.pass);

				SSLContext ssc = SSLContext.getInstance("TLS");

				// initialize SSL with certificate and the trusted CA and pub
				// certs
				ssc.init(kmf.getKeyManagers(), JAKeyStore.trusts,
						new SecureRandom());

				SSLSocketFactory f = ssc.getSocketFactory();

				SSLSocket client = (SSLSocket) f.createSocket(address, p);
				
				// print info
				printSocketInfo(client);
				
				client.startHandshake();

				X509Certificate[] peerCerts =

				client.getSession().getPeerCertificateChain();

				if (peerCerts != null) {

					logger.log(Level.INFO,"Printing peer's information:");

					for (int i = 0; i < peerCerts.length; i++) {

						logger.log(Level.INFO,
								"Peer's Certificate Information:\n"
										+ Level.INFO, "- Subject: "
										+ peerCerts[i].getSubjectDN().getName()
										+ "\n"
										+ peerCerts[i].getIssuerDN().getName()
										+ "\n"
										+ Level.INFO
										+ "- Start Time: "
										+ peerCerts[i].getNotBefore()
												.toString() + "\n" + Level.INFO
										+ "- End Time: "
										+ peerCerts[i].getNotAfter().toString()

						);
					}

					DispatchSSLClient sc = new DispatchSSLClient(client);

					instance.put(Integer.valueOf(p), sc);

				} else
					logger.log(Level.SEVERE,"We didn't get any peer/service cert. NOT GOOD!");

			} catch (Exception e) {
				logger.log(Level.SEVERE,"Could not initiate SSL connection to the server.");
				e.printStackTrace();
			}

		}
		
		return instance.get(Integer.valueOf(p));
	}

	
	@SuppressWarnings("unused")
	private void close() {
		if (ois != null) {
			try {
				ois.close();
			} catch (IOException ioe) {
				// ignore
			}
		}

		if (oos != null) {
			try {
				oos.close();
			} catch (IOException ioe) {
				// ignore
			}
		}

		if (connection != null) {
			try {
				connection.close();
			} catch (IOException ioe) {
				// ignore
			}
		}

		instance = null;
	}

	
	/**
	 * Total amount of time (in milliseconds) spent in writing objects to the
	 * socket.
	 */
	public static long lSerialization = 0;

	
	private static synchronized void initializeSocketInfo(){
		addr = ConfigUtils.getConfig().gets(serviceName).trim();

		if (addr.length() == 0) {
			addr = defaultHost;
			port = defaultPort;
		} else {

			String address = addr;
			int idx = address.indexOf(':');

			if (idx >= 0) {
				try {
					port = Integer.parseInt(address.substring(idx + 1));
					addr = address.substring(0, idx);
				} catch (Exception e) {
					addr = defaultHost;
					port = defaultPort;
				}
			}
		}
	}
	
	
	/**
	 * @param r
	 * @return 
	 *             in case of connectivity problems
	 */
	public static synchronized Request dispatchRequest(final Request r){
	
			initializeSocketInfo();
			try{
				return dispatchARequest(r);
			}
			catch (IOException e){
				// Now let's try, if we can reconnect
				instance.put(Integer.valueOf(port), null);
				try {
					return dispatchARequest(r);
				} catch (IOException e1) {
					// This time we give up
					logger.log(Level.SEVERE,"Error running request, potential connection error.");
					return null;
				}
			}
		
	}
	/**
	 * @param r
	 * @return the processed request, if successful
	 * @throws IOException
	 *             in case of connectivity problems
	 */
	public static synchronized Request dispatchARequest(final Request r)
			throws IOException {
	
		final DispatchSSLClient c = getInstance(addr, port);

		if (c == null)
			throw new IOException("Connection is null");

			long lStart = System.currentTimeMillis();

			c.oos.writeObject(r);
			
			if (++c.objectsSentCounter >= RESET_OBJECT_STREAM_COUNTER){
				c.oos.reset();
				c.objectsSentCounter = 0;
			}
			
			c.oos.flush();
			c.os.flush();

			lSerialization += System.currentTimeMillis() - lStart;

			Object o;
			try {
				o = c.ois.readObject();
			} catch (ClassNotFoundException e) {
				throw new IOException(e.getMessage());
			}

			return (Request) o;
		
	}

	private static void printSocketInfo(SSLSocket s) {
		
		logger.log(Level.INFO,"Remote address: " + s.getInetAddress().toString() +":"+ s.getPort());
		logger.log(Level.INFO,"   Local socket address = "
				+ s.getLocalSocketAddress().toString());

		logger.log(Level.INFO,"   Cipher suite = " + s.getSession().getCipherSuite());
		logger.log(Level.INFO,"   Protocol = " + s.getSession().getProtocol());
	}

}
