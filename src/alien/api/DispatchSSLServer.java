package alien.api;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateException;
import java.util.Arrays;
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
import alien.user.AliEnPrincipal;
import alien.user.JAKeyStore;
import alien.user.UserFactory;

/**
 * @author costing
 * 
 */
public class DispatchSSLServer extends Thread {

	/**
	 * Reset the object stream every this many objects sent
	 */
	private static final int RESET_OBJECT_STREAM_COUNTER = 1000;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(DispatchSSLServer.class.getCanonicalName());

	/**
	 * The entire connection
	 */
	private final Socket connection;

	/**
	 * Getting requests by this stream
	 */
	private ObjectInputStream ois;

	/**
	 * Writing replies here
	 */
	private ObjectOutputStream oos;

	private OutputStream os;

	private X509Certificate partnerCerts[] = null;

	private static final int defaultPort = 5282;
	private static String serviceName = "apiService";

	private static boolean forwardRequest = false;

	private int objectsSentCounter = 0;

	/**
	 * E.g. the CE proxy should act as a fowarding bridge between JA and central services
	 * 
	 * @param servName
	 *            name of the config parameter for the host:port settings
	 */
	public static void overWriteServiceAndForward(final String servName) {
		// TODO: we could drop the serviceName overwrite, once we assume to run
		// not on one single host everything
		serviceName = servName;
		forwardRequest = true;
	}

	/**
	 * @param connection
	 * @throws IOException
	 */
	public DispatchSSLServer(final Socket connection) throws IOException {
		this.connection = connection;

		setName(connection.getInetAddress().toString());
		setDaemon(true);
	}

	@Override
	public void run() {
		try {
			connection.setTcpNoDelay(true);
			connection.setTrafficClass(0x10);

			this.os = connection.getOutputStream();

			this.oos = new ObjectOutputStream(this.os);
			this.oos.flush();
			this.os.flush();

			this.ois = new ObjectInputStream(connection.getInputStream());
		} catch (final IOException e) {
			logger.log(Level.WARNING, "Exception initializing the SSL socket", e);
			return;
		}

		long lLasted = 0;

		try {
			while (true) {
				final Object o = ois.readObject();

				if (o != null)
					if (o instanceof Request) {
						Request r = (Request) o;

						final long lStart = System.currentTimeMillis();

						final AliEnPrincipal remoteIdentity = UserFactory.getByCertificate(partnerCerts);

						if (remoteIdentity == null) {
							logger.log(Level.WARNING, "Could not get the identity of this certificate chain: " + Arrays.toString(partnerCerts));
							return;
						}

						remoteIdentity.setRemoteEndpoint(connection.getInetAddress());

						r.setPartnerIdentity(remoteIdentity);

						r.setPartnerCertificate(partnerCerts);

						if (forwardRequest)
							r = DispatchSSLClient.dispatchRequest(r);
						else {
							r.authorizeUserAndRole();

							try {
								r.run();
							} catch (final Exception e) {
								logger.log(Level.WARNING, "Returning an exception to the client", e);

								r.setException(new ServerException(e.getMessage(), e));
							}
						}

						lLasted += (System.currentTimeMillis() - lStart);

						final long lSer = System.currentTimeMillis();

						// System.err.println("When returning the object, ex is "+r.getException());

						oos.writeObject(r);

						if (++objectsSentCounter >= RESET_OBJECT_STREAM_COUNTER) {
							oos.reset();
							objectsSentCounter = 0;
						}

						oos.flush();
						os.flush();

						lSerialization += System.currentTimeMillis() - lSer;

						logger.log(Level.INFO, "Got request from " + r.getRequesterIdentity() + " : " + r.getClass().getCanonicalName()); // +
																																			// ": "+r.toString());

					} else
						logger.log(Level.WARNING, "I don't know what to do with an object of type " + o.getClass().getCanonicalName());
			}
		} catch (final Throwable e) {
			logger.log(Level.WARNING, "Lasted : " + lLasted + ", serialization : " + lSerialization, e);
		} finally {
			if (ois != null)
				try {
					ois.close();
				} catch (final IOException ioe) {
					// ignore
				}

			if (oos != null)
				try {
					oos.close();
				} catch (final IOException ioe) {
					// ignore
				}

			if (connection != null)
				try {
					connection.close();
				} catch (final IOException ioe) {
					// ignore
				}
		}
	}

	/**
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	public static void runService() throws IOException {

		int port = 0;

		final String address = ConfigUtils.getConfig().gets(serviceName).trim();

		if (address.length() != 0) {

			final int idx = address.indexOf(':');

			if (idx >= 0)
				try {
					port = Integer.parseInt(address.substring(idx + 1));
					// address = address.substring(0, idx);
				} catch (final Exception e) {
					port = defaultPort;
				}
		}

		SSLServerSocket server = null;

		try {
			Security.addProvider(new BouncyCastleProvider());

			final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509", "SunJSSE");

			kmf.init(JAKeyStore.hostCert, JAKeyStore.pass);

			String logCertInfo = "Running JCentral with host cert: ";

			try {
				((java.security.cert.X509Certificate) JAKeyStore.hostCert.getCertificateChain("Host.cert")[0]).checkValidity();
			} catch (final CertificateException e) {
				logCertInfo = "Our host certificate expired or is invalid!";
			}
			logCertInfo += ((java.security.cert.X509Certificate) JAKeyStore.hostCert.getCertificateChain("Host.cert")[0]).getSubjectDN();

			System.out.println(logCertInfo);

			logger.log(Level.INFO, logCertInfo);

			final SSLContext sc = SSLContext.getInstance("TLS");

			final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
			tmf.init(JAKeyStore.hostCert);

			sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

			final SSLServerSocketFactory ssf = sc.getServerSocketFactory();

			server = (SSLServerSocket) ssf.createServerSocket(port);

			server.setWantClientAuth(true);
			server.setNeedClientAuth(true);

			server.setUseClientMode(false);

			System.out.println("JCentral listening on " + server.getLocalPort());

			logger.log(Level.INFO, "JCentral listening on  " + server.getLocalPort());

			while (true)
				try {
					@SuppressWarnings("resource")
					// this object is passed to another thread to deal with the
					// communication
					final SSLSocket c = (SSLSocket) server.accept();

					if (server.getNeedClientAuth() == true) {

						logger.log(Level.INFO, "Printing client information:");
						final X509Certificate[] peerCerts = c.getSession().getPeerCertificateChain();

						if (peerCerts != null)
							for (final X509Certificate peerCert : peerCerts)
								logger.log(Level.INFO, printClientInfo(peerCert));
						else
							logger.log(Level.INFO, "Failed to get peer certificates");
					}

					final DispatchSSLServer serv = new DispatchSSLServer(c);
					if (server.getNeedClientAuth() == true)
						serv.partnerCerts = c.getSession().getPeerCertificateChain();

					serv.start();
				} catch (final IOException ioe) {
					logger.log(Level.WARNING, "Exception treating a client", ioe);
				}

		} catch (final Throwable e) {
			logger.log(Level.SEVERE, "Could not initiate SSL Server Socket.", e);
		}
	}

	/**
	 * Total amount of time (in milliseconds) spent in writing objects to the socket.
	 */
	public static long lSerialization = 0;

	/**
	 * Print client info on SSL partner
	 */
	private static String printClientInfo(final X509Certificate peerCerts) {
		return "Peer Certificate Information:\n" + "- Subject: " + peerCerts.getSubjectDN().getName() + "- Issuer: \n" + peerCerts.getIssuerDN().getName() + "- Version: \n" + peerCerts.getVersion()
				+ "- Start Time: \n" + peerCerts.getNotBefore().toString() + "\n" + "- End Time: " + peerCerts.getNotAfter().toString() + "\n" + "- Signature Algorithm: " + peerCerts.getSigAlgName()
				+ "\n" + "- Serial Number: " + peerCerts.getSerialNumber();
	}

	/**
	 * Print some info on the SSL Socket
	 */
	// private static String printServerSocketInfo(SSLServerSocket s) {
	// return "Server socket class: " + s.getClass()
	// + "\n   Socket address = " + s.getInetAddress().toString()
	// + "\n   Socket port = " + s.getLocalPort()
	// + "\n   Need client authentication = " + s.getNeedClientAuth()
	// + "\n   Want client authentication = " + s.getWantClientAuth()
	// + "\n   Use client mode = " + s.getUseClientMode();
	// }

}
