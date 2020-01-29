package alien.api;

import java.io.File;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Service;
import org.apache.catalina.authenticator.SSLAuthenticator;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.servlets.DefaultServlet;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.descriptor.web.LoginConfig;
import org.apache.tomcat.util.descriptor.web.SecurityCollection;
import org.apache.tomcat.util.descriptor.web.SecurityConstraint;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.JAKeyStore;
import alien.user.LdapCertificateRealm;
import alien.user.UserFactory;
import alien.websockets.WebsocketListener;

/**
 * @author vyurchen
 *
 */
public class TomcatServer {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(JBoxServer.class.getCanonicalName());

	static final Monitor monitor = MonitorFactory.getMonitor(TomcatServer.class.getCanonicalName());

	/**
	 * Web server instance
	 */
	final Tomcat tomcat;

	private final int websocketPort;

	/**
	 * Start the Tomcat server on a given port
	 *
	 * @param tomcatPort
	 */
	private TomcatServer(final int tomcatPort, final String bindAddress) throws Exception {
		this.websocketPort = tomcatPort;

		tomcat = new Tomcat();
		tomcat.setBaseDir(System.getProperty("java.io.tmpdir"));
		tomcat.setPort(tomcatPort);
		final Service service = tomcat.getService();
		tomcat.getService().removeConnector(tomcat.getConnector()); // remove default connector
		service.addConnector(createSslConnector(tomcatPort, bindAddress));

		// Add an empty Tomcat context
		final Context ctx = tomcat.addContext("", null);

		// Configure websocket context listener
		ctx.addApplicationListener(WebsocketListener.class.getName());
		Tomcat.addServlet(ctx, "default", new DefaultServlet());
		ctx.addServletMappingDecoded("/", "default");

		// Set security constraints in order to use AlienUserPrincipal later
		final SecurityCollection securityCollection = new SecurityCollection();
		securityCollection.addPattern("/*");
		final SecurityConstraint securityConstraint = new SecurityConstraint();
		securityConstraint.addCollection(securityCollection);
		securityConstraint.setAuthConstraint(true);
		securityConstraint.setUserConstraint("CONFIDENTIAL");
		securityConstraint.addAuthRole("users");
		ctx.addConstraint(securityConstraint);

		final LoginConfig loginConfig = new LoginConfig();
		loginConfig.setAuthMethod("CLIENT-CERT");
		loginConfig.setRealmName(LdapCertificateRealm.class.getCanonicalName());
		ctx.setLoginConfig(loginConfig);
		final LdapCertificateRealm ldapRealm = new LdapCertificateRealm();
		ctx.setRealm(ldapRealm);
		ctx.getPipeline().addValve(new SSLAuthenticator());

		tomcat.start();
		if (tomcat.getService().findConnectors()[0].getState() == LifecycleState.FAILED)
			throw new BindException();

		final Executor executor = tomcat.getConnector().getProtocolHandler().getExecutor();

		if (executor instanceof ThreadPoolExecutor) {
			final ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;

			monitor.addMonitoring("server_status", (names, values) -> {
				names.add("active_threads");
				values.add(Double.valueOf(tpe.getActiveCount()));

				names.add("max_threads");
				values.add(Double.valueOf(tpe.getMaximumPoolSize()));
			});
		}
		else
			logger.log(Level.SEVERE, "Cannot monitor Tomcat executor of type " + executor.getClass().getCanonicalName());

		// Let Tomcat run in another thread so it will keep on waiting forever
		new Thread() {
			@Override
			public void run() {
				tomcat.getServer().await();
			}
		}.start();
	}

	/**
	 * Create SSL connector for the Tomcat server
	 *
	 * @param tomcatPort
	 */
	private static Connector createSslConnector(final int tomcatPort, final String bindAddress) {
		final String keystorePass = new String(JAKeyStore.pass);

		final String dirName = System.getProperty("java.io.tmpdir") + File.separator;
		final String keystoreName = dirName + "keystore.jks_" + UserFactory.getUserID();
		final String truststoreName = dirName + "truststore.jks_" + UserFactory.getUserID();

		if (ConfigUtils.isCentralService())
			JAKeyStore.saveKeyStore(JAKeyStore.getKeyStore(), keystoreName, JAKeyStore.pass);
		else
			JAKeyStore.saveKeyStore(JAKeyStore.tokenCert, keystoreName, JAKeyStore.pass);

		JAKeyStore.saveKeyStore(JAKeyStore.trustStore, truststoreName, JAKeyStore.pass);

		final Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");

		connector.setAttribute("address", ConfigUtils.getConfig().gets("alien.api.TomcatServer.bindAddress", bindAddress));

		connector.setPort(tomcatPort);
		connector.setSecure(true);
		connector.setScheme("https");
		connector.setAttribute("keyAlias", "User.cert");
		connector.setAttribute("keystorePass", keystorePass);
		connector.setAttribute("keystoreType", "JKS");
		connector.setAttribute("keystoreFile", keystoreName);
		connector.setAttribute("truststorePass", keystorePass);
		connector.setAttribute("truststoreType", "JKS");
		connector.setAttribute("truststoreFile", truststoreName);
		connector.setAttribute("clientAuth", "true");
		connector.setAttribute("sslProtocol", "TLS");
		connector.setAttribute("SSLEnabled", "true");
		connector.setAttribute("maxThreads", "200");
		connector.setAttribute("connectionTimeout", "20000");
		return connector;
	}

	/**
	 * Singleton
	 */
	static TomcatServer tomcatServer = null;

	/**
	 * Start Tomcat Server
	 */
	public static synchronized void startTomcatServer() {

		if (tomcatServer != null)
			return;

		logger.log(Level.INFO, "Tomcat starting ...");

		if (ConfigUtils.isCentralService()) {
			// Try to launch Tomcat on default port
			final int port = ConfigUtils.getConfig().geti("alien.api.TomcatServer.csPort", 8097);

			try (ServerSocket ssocket = new ServerSocket(port)) // Fast check if port is available
			{
				ssocket.close();
				// Actually start Tomcat
				tomcatServer = new TomcatServer(port, "*");

				logger.log(Level.INFO, "Tomcat listening on " + getListeningAddressAndPort());
				System.out.println("Tomcat is listening on " + getListeningAddressAndPort());
			}
			catch (final Exception ioe) {
				// Central services listen on a fixed server port, they should not bind on a different one as a fallback
				logger.log(Level.SEVERE, "Tomcat: Could not listen on CS port " + port, ioe);
			}
		}
		else {
			// Set dynamic port range for Tomcat server
			final int portMin = Integer.parseInt(ConfigUtils.getConfig().gets("port.range.start", "10100"));
			final int portMax = Integer.parseInt(ConfigUtils.getConfig().gets("port.range.end", "10700"));

			// Try another ports in range
			for (int port = portMin; port < portMax; port++)
				try (ServerSocket ssocket = new ServerSocket(port, 1, InetAddress.getByName("localhost"))) // Fast check if port is available
				{
					ssocket.close();
					// Actually start Tomcat
					tomcatServer = new TomcatServer(port, "localhost");

					logger.log(Level.INFO, "Tomcat listening on port " + getListeningAddressAndPort());
					System.out.println("Tomcat is listening on " + getListeningAddressAndPort());
					break;
				}
				catch (final Exception ioe) {
					// Try next one
					logger.log(Level.FINE, "Tomcat: Could not listen on port " + port, ioe);
				}
		}
	}

	/**
	 *
	 * Get the port used by tomcatServer
	 *
	 * @return the TCP port this server is listening on. Can be negative to signal that the server is actually not listening on any port (yet?)
	 *
	 */
	public static int getPort() {
		return tomcatServer != null ? tomcatServer.websocketPort : -1;
	}

	/**
	 * @return the address:port where Tomcat is listening, or <code>null</code> if the server is not started
	 */
	public static String getListeningAddressAndPort() {
		if (tomcatServer != null) {
			final Object addressAttribute = tomcatServer.tomcat.getConnector().getAttribute("address");

			String address = addressAttribute != null ? addressAttribute.toString() : "*";

			address += ":" + tomcatServer.tomcat.getConnector().getPort();

			return address;
		}

		return null;
	}
}
