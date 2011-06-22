package alien.api;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.SecureRandom;
import java.security.Security;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.security.cert.X509Certificate;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import alien.config.ConfigUtils;
import alien.user.JAKeyStore;
import alien.user.UserFactory;

/**
 * @author costing
 * 
 */
public class DispatchSSLServer extends Thread {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(DispatchSSLServer.class.getCanonicalName());

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

	private X509Certificate partnerCerts[] = null;

	private static final int defaultPort = 5282;
	private static final String serviceName = "apiService";

	/**
	 * @param connection
	 * @throws IOException
	 */
	public DispatchSSLServer(final Socket connection) throws IOException {
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

		try {
			while (true) {
				final Object o = ois.readObject();

				if (o != null) {
					if (o instanceof Request) {
						final Request r = (Request) o;

						long lStart = System.currentTimeMillis();

						r.setPartnerIdentity(UserFactory.getByCertificate(partnerCerts));
						r.setPartnerCertificate(partnerCerts);

						r.run();

						lLasted += (System.currentTimeMillis() - lStart);

						long lSer = System.currentTimeMillis();

						oos.writeObject(r);
						oos.flush();
						os.flush();

						lSerialization += System.currentTimeMillis() - lSer;

						System.out.println("Got request from "
								+ r.getRequesterIdentity() + " : "
								+ r.getClass().getCanonicalName()); // +
																	// ": "+r.toString());

					} else {
						logger.log(Level.WARNING,
								"I don't know what to do with an object of type "
										+ o.getClass().getCanonicalName());
					}
				}
			}
		} catch (Exception e) {
			System.err.println("Lasted : " + lLasted + ", serialization : "
					+ lSerialization);

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
		}
	}

	/**
	 * @throws IOException
	 */
	public static void runService() throws IOException {

		int port = 0;

		String address = ConfigUtils.getConfig().gets(serviceName).trim();

		if (address.length() != 0) {

			int idx = address.indexOf(':');

			if (idx >= 0) {
				try {
					port = Integer.parseInt(address.substring(idx + 1));
					address = address.substring(0, idx);
				} catch (Exception e) {
					port = defaultPort;
				}
			}
		}

		SSLServerSocket server = null;

		try {

			Security.addProvider(new BouncyCastleProvider());

			KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509",
					"SunJSSE");

			kmf.init(JAKeyStore.hostCert, JAKeyStore.pass);

			System.out.println("Running central service with host cert: "
					+ ((java.security.cert.X509Certificate) JAKeyStore.hostCert
							.getCertificateChain("Host.cert")[0])
							.getSubjectDN());

			SSLContext sc = SSLContext.getInstance("TLS");

			TrustManagerFactory tmf = TrustManagerFactory
					.getInstance("SunX509");
			tmf.init(JAKeyStore.hostCert);

			sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(),
					new SecureRandom());

			SSLServerSocketFactory ssf = sc.getServerSocketFactory();

			server = (SSLServerSocket) ssf.createServerSocket(port);

			server.setNeedClientAuth(true);

			server.setUseClientMode(false);

			printServerSocketInfo(server);

			System.out.println("Central service listening now on " + server.getLocalPort());
			
			logger.log(Level.INFO, "Central service listening now on  "+server.getLocalPort());

			while (true) {
				try {

					final SSLSocket c = (SSLSocket) server.accept();

					if (server.getNeedClientAuth() == true) {
						
						System.out.println("Printing client information:");
						X509Certificate[] peerCerts = c.getSession()
								.getPeerCertificateChain();

						if (peerCerts != null) {
							for (int i = 0; i < peerCerts.length; i++) {
								System.out.println("Peer Certificate [" + i
										+ "] Information:");
								System.out
										.println("- Subject: "
												+ peerCerts[i].getSubjectDN()
														.getName());
								System.out.println("- Issuer: "
										+ peerCerts[i].getIssuerDN().getName());
								System.out.println("- Version: "
										+ peerCerts[i].getVersion());
								System.out.println("- Start Time: "
										+ peerCerts[i].getNotBefore()
												.toString());
								System.out
										.println("- End Time: "
												+ peerCerts[i].getNotAfter()
														.toString());
								System.out.println("- Signature Algorithm: "
										+ peerCerts[i].getSigAlgName());
								System.out.println("- Serial Number: "
										+ peerCerts[i].getSerialNumber());

							}
						} else
							System.out
									.println("Failed to get peer certificates");
					}

					DispatchSSLServer serv = new DispatchSSLServer(c);
					if (server.getNeedClientAuth() == true)
						serv.partnerCerts = c.getSession()
					.getPeerCertificateChain();
					
					
					
					serv.start();

				} catch (IOException ioe) {
					logger.log(Level.WARNING, "Exception treating a client",
							ioe);
				}
			}

		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not initiate SSL Server Socket.",
					e);
		}
	}

	
	/**
	 * Total amount of time (in milliseconds) spent in writing objects to the
	 * socket.
	 */
	public static long lSerialization = 0;

	
	/**
	 * Print some info on the SSL Socket
	 */
	private static void printServerSocketInfo(SSLServerSocket s) {
		System.out.println("Server socket class: " + s.getClass());
		System.out.println("   Socket address = "
				+ s.getInetAddress().toString());
		System.out.println("   Socket port = " + s.getLocalPort());
		System.out.println("   Need client authentication = "
				+ s.getNeedClientAuth());
		System.out.println("   Want client authentication = "
				+ s.getWantClientAuth());
		System.out.println("   Use client mode = " + s.getUseClientMode());
	}

}
