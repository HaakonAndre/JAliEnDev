package alien.api;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.BindException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Service;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.scan.StandardJarScanner;

import alien.config.ConfigUtils;
import alien.user.JAKeyStore;
import lazyj.commands.SystemCommand;

public class TomcatServer {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JBoxServer.class.getCanonicalName());
			
	private Tomcat tomcat;
	
	private final int websocketPort;
	
	/**
	 * Start the Tomcat server on a given port
	 *
	 * @param tomcatPort
	 */
	private TomcatServer(int tomcatPort, int iDebug) throws Exception {		
		this.websocketPort = tomcatPort;
		
        tomcat = new Tomcat();
        Service service = tomcat.getService();
        tomcat.getService().removeConnector(tomcat.getConnector()); // remove default connector	       	        
        service.addConnector(createSslConnector(tomcatPort));     
        
        // Configure websocket webapplication
        String webappDirLocation = "src/alien/websockets";
        Context ctx = tomcat.addWebapp("", new File(webappDirLocation).getAbsolutePath());
        
        // Tell Jar Scanner not to look inside jar manifests otherwise it will produce useless warnings
        StandardJarScanner jarScanner = (StandardJarScanner) ctx.getJarScanner();
        jarScanner.setScanManifest(false);      	        
        
        tomcat.start();
        if (tomcat.getService().findConnectors()[0].getState() == LifecycleState.FAILED)
        	throw new BindException();
	            
        if (!writeTokenFile(websocketPort, iDebug)) {
        	tomcat.stop();
			throw new Exception("Could not write the token file! No application can connect to JBox");
		}
	}
	
	/**
	 * Create connector for the Tomcat server
	 *
	 * @param tomcatPort
	 * @throws Exception 
	 * @throws IOException
	 */
	private Connector createSslConnector(int tomcatPort) throws Exception {
		String keystorePass = new String(JAKeyStore.pass);
		JAKeyStore.saveKeyStore(JAKeyStore.clientCert, "keystore.jks", JAKeyStore.pass);

	    Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
	
	    connector.setPort(tomcatPort);
	    connector.setSecure(true);
	    connector.setScheme("https");
	    connector.setAttribute("keyAlias", "User.cert");
	    connector.setAttribute("keystorePass", keystorePass);
	    connector.setAttribute("keystoreType", "JKS");
	    connector.setAttribute("keystoreFile", System.getProperty("user.dir") + System.getProperty("file.separator") + "keystore.jks");
	    connector.setAttribute("truststorePass", "castore");
	    connector.setAttribute("truststoreType", "JKS");
	    connector.setAttribute("truststoreFile", System.getProperty("user.dir") + System.getProperty("file.separator") + "trusted_authorities.jks");
	    connector.setAttribute("clientAuth", "true");
	    connector.setAttribute("sslProtocol", "TLS");
	    connector.setAttribute("SSLEnabled", "true");
	    connector.setAttribute("maxThreads", "200");
	    connector.setAttribute("connectionTimeout", "20000");
	    return connector; 
	}
	
	private static boolean writeTokenFile(final int iWSPort, final int iDebug) {
		String sUserId = System.getProperty("userid");

		if (sUserId == null || sUserId.length() == 0) {
			sUserId = SystemCommand.bash("id -u " + System.getProperty("user.name")).stdout;

			if (sUserId != null && sUserId.length() > 0)
				System.setProperty("userid", sUserId);
			else {
				logger.severe("User Id empty! Could not get the token file name");
				return false;
			}
		}
		
		try {
			final int iUserId = Integer.parseInt(sUserId.trim());

			final File tmpDir = new File(System.getProperty("java.io.tmpdir"));

			final File tokenFile = new File(tmpDir, "jclient_token_" + iUserId);

			try (FileWriter fw = new FileWriter(tokenFile, true)) {			
				fw.write("WSPort=" + iWSPort + "\n");
				logger.fine("WSPort = " + iWSPort);

				fw.flush();
				fw.close();

				return true;

			} catch (final Exception e1) {
				logger.log(Level.SEVERE, "Could not open file " + tokenFile + " to write", e1);
				return false;
			}
		} catch (final Throwable e) {
			logger.log(Level.SEVERE, "Could not get user id! The token file could not be created ", e);

			return false;
		}
	}
	
	/**
	 * Singleton
	 */
	static TomcatServer tomcatServer = null;

	/**
	 * Start once the UIServer
	 *
	 * @param iDebugLevel
	 * @throws Exception 
	 */
	public static synchronized void startTomcatServer(final int iDebugLevel) {

		logger.log(Level.INFO, "Tomcat starting ...");

		if (tomcatServer != null)
			return;

		int portMin = Integer.parseInt(ConfigUtils.getConfig().gets("port.range.start", "10100"));
		int portMax = Integer.parseInt(ConfigUtils.getConfig().gets("port.range.end", "10200"));
		Boolean portAny = Boolean.valueOf(ConfigUtils.getConfig().gets("port.range.any", "true"));

		for (int port = portMin; port < portMax; port++)
			try {
				if (portAny) port = 8098;
	
				tomcatServer = new TomcatServer(port, iDebugLevel);

				logger.log(Level.INFO, "Tomcat listening on port " + port);
				System.out.println("Tomcat is listening on port " + port);

				break;
			} catch (final Exception ioe) {
				// we don't need the already in use info on the port, maybe
				// there's another user on the machine...
				logger.log(Level.FINE, "Tomcat: Could not listen on port " + port, ioe);
			}
	}
}
