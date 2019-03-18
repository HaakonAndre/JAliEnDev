package alien.api;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JShPrintWriter;
import alien.shell.commands.UIPrintWriter;
import alien.shell.commands.XMLPrintWriter;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import alien.user.UsersHelper;

/**
 * Simple UI server to be used by ROOT and command line
 *
 * @author costing
 */
public class JBoxServer extends Thread {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JBoxServer.class.getCanonicalName());

	// this triggers to ask for the user home LFN before doing anything else
	private static boolean preemptJCentralConnection = true;

	/**
	 *
	 */
	public static final String passACK = "OKPASSACK";

	/**
	 * static{ new }
	 */
	public static final String passNOACK = "NOPASSACK";

	private final int port;

	private final ServerSocket ssocket;

	private UIConnection connection;

	/**
	 * The password
	 */
	final String password;

	/**
	 * Debug level received from the user
	 */
	final int iDebugLevel;

	/**
	 * Number of currently established client connections to this instance
	 */
	final static AtomicInteger connectedClients = new AtomicInteger(0);

	/**
	 * Timestamp of the last operation (connect / disconnect / command)
	 */
	static long lastOperation = System.currentTimeMillis();

	/**
	 * On any activity update the {@link #lastOperation} field
	 */
	static void notifyActivity() {
		lastOperation = System.currentTimeMillis();
	}

	static final class ShutdownThread extends Thread {
		public ShutdownThread() {
			setName("Idle server watcher");
		}

		@Override
		public void run() {
			while (true) {
				if (connectedClients.get() == 0 && (System.currentTimeMillis() - lastOperation) > 1000 * 60 * 60 * 24 && server != null)
					server.shutdown();

				try {
					Thread.sleep(1000 * 60);
				}
				catch (@SuppressWarnings("unused") final InterruptedException ie) {
					// it's interrupted when it should exit
					break;
				}
			}
		}
	}

	private static ShutdownThread shutdownThread = null;

	private static synchronized void preempt() {
		if (preemptJCentralConnection) {
			final PreemptJCentralConnection preempt = new PreemptJCentralConnection();
			preempt.start();
			preemptJCentralConnection = false;
		}
	}

	private static synchronized void startShutdownThread() {
		if (shutdownThread == null) {
			shutdownThread = new ShutdownThread();

			shutdownThread.start();
		}
	}

	/**
	 * Start the server on a given port
	 *
	 * @param listeningPort
	 * @throws IOException
	 */
	private JBoxServer(final int listeningPort, final int iDebug) throws Exception {
		final AliEnPrincipal alUser = AuthorizationFactory.getDefaultUser();

		if (alUser == null || alUser.getName() == null)
			throw new Exception("Could not get your username. FATAL!");

		final InetAddress localhost = InetAddress.getByName("127.0.0.1");

		ssocket = new ServerSocket(listeningPort, 10, localhost);
		this.port = ssocket.getLocalPort();
		this.iDebugLevel = iDebug;

		password = UUID.randomUUID().toString();

		// here we should get home directory
		final String sHomeUser = UsersHelper.getHomeDir(alUser.getName());

		// should check if the file was written and if not then exit.
		if (!writeTokenFile("127.0.0.1", this.port, password, alUser.getName(), sHomeUser, this.iDebugLevel)) {
			ssocket.close();
			throw new Exception("Could not write the token file! No application can connect to JBox");
		}

		if (!writeEnvFile("127.0.0.1", this.port, alUser.getName())) {
			ssocket.close();
			throw new Exception("Could not write the env file! JSh/JRoot will not be able to connect to JBox");
		}

		startShutdownThread();
	}

	/**
	 * write the configuration file that is used by gapi <br />
	 * the filename = <i>java.io.tmpdir</i>/jclient_token_$uid
	 *
	 * @param sHost
	 *            hostname to connect to, by default localhost
	 * @param iPort
	 *            port number for listening
	 * @param sPassword
	 *            the password used by other application to connect to the JBoxServer
	 * @param sUser
	 *            the user from the certificate
	 * @param iDebug
	 *            the debug level received from the command line
	 * @return true if the file was written, false if not
	 * @author Alina Grigoras
	 */
	private static boolean writeTokenFile(final String sHost, final int iPort, final String sPassword, final String sUser, final String sHomeUser, final int iDebug) {

		try {
			final String sUserId = UserFactory.getUserID();

			if (sUserId == null) {
				logger.log(Level.SEVERE, "Cannot get the current user's ID");
				return false;
			}

			final int iUserId = Integer.parseInt(sUserId.trim());

			final File tmpDir = new File(System.getProperty("java.io.tmpdir"));

			final File tokenFile = new File(tmpDir, "jclient_token_" + iUserId);

			try (FileWriter fw = new FileWriter(tokenFile)) {
				fw.write("Host=" + sHost + "\n");
				logger.fine("Host = " + sHost);

				fw.write("Port=" + iPort + "\n");
				logger.fine("Port = " + iPort);

				fw.write("User=" + sUser + "\n");
				logger.fine("User = " + sUser);

				fw.write("Home=" + sHomeUser + "\n");
				logger.fine("Home = " + sHomeUser);

				fw.write("Passwd=" + sPassword + "\n");
				logger.fine("Passwd = " + sPassword);

				fw.write("Debug=" + iDebug + "\n");
				logger.fine("Debug = " + iDebug);

				fw.write("PID=" + MonitorFactory.getSelfProcessID() + "\n");
				logger.fine("PID = " + MonitorFactory.getSelfProcessID());

				fw.flush();
				fw.close();

				return true;

			}
			catch (final Exception e1) {
				logger.log(Level.SEVERE, "Could not open file " + tokenFile + " to write", e1);
				return false;
			}
		}
		catch (final Throwable e) {
			logger.log(Level.SEVERE, "Could not get user id! The token file could not be created ", e);

			return false;
		}
	}

	/**
	 * Writes the environment file used by ROOT <br />
	 * It needs to be named jclient_env_$UID, sitting by default in <code>java.io.tmpdir</code> (eg. <code>/tmp</code>) and to contain:
	 * <ol>
	 * <li>alien_API_HOST</li>
	 * <li>alien_API_PORT</li>
	 * <li>alien_API_USER</li>
	 * <li>LD/DYLD_LIBRARY_PATH</li>
	 * </ol>
	 *
	 * @param iPort
	 * @param sPassword
	 * @param sUser
	 * @return <code>true</code> if everything went fine, <code>false</code> if there was an error writing the env file
	 */
	private static boolean writeEnvFile(final String sHost, final int iPort, final String sUser) {
		final String sUserId = System.getProperty("userid");

		if (sUserId == null || sUserId.length() == 0) {
			logger.severe("User Id empty! Could not get the env file name");
			return false;
		}

		// String sAlienRoot = System.getenv("ALIEN_ROOT");
		//
		// if(sAlienRoot == null || sAlienRoot.length() ==0 ){
		// logger.severe("No ALIEN_ROOT found. Please set ALIEN_ROOT environment variable");
		// System.out.println("You don't have $ALIEN_ROOT set. You will not be able to copy files.");
		// }

		try {
			final int iUserId = Integer.parseInt(sUserId.trim());

			final File tmpDir = new File(System.getProperty("java.io.tmpdir"));

			final File envFile = new File(tmpDir, "jclient_env_" + iUserId);

			try (FileWriter fw = new FileWriter(envFile)) {
				fw.write("export alien_API_HOST=" + sHost + "\n");
				logger.fine("export alien_API_HOST=" + sHost);

				fw.write("export alien_API_PORT=" + iPort + "\n");
				logger.fine("export alien_API_PORT=" + iPort);

				fw.write("export alien_API_USER=" + sUser + "\n");
				logger.fine("export alien_API_USER=" + sUser);

				fw.write("export JROOT=1\n");
				logger.fine("export JROOT=1");

				fw.flush();
				fw.close();

				return true;

			}
			catch (final Exception e1) {
				logger.log(Level.SEVERE, "Could not open file " + envFile.getAbsolutePath() + " to write", e1);
				return false;
			}
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Could not get user id! The env file could not be created ", e);
			return false;
		}
	}

	/**
	 * ramp up the ssl to JCentral
	 *
	 * @author gron
	 */
	static final class PreemptJCentralConnection extends Thread {
		@Override
		public void run() {
			if (SEUtils.getSE(0) == null) {
				logger.log(Level.SEVERE, "JBoxServer: Error running request, potential connection error.");
				System.err.println("JBoxServer: Error running request, potential connection error.");
			}
		}
	}

	/**
	 * One UI connection
	 *
	 * @author costing
	 */
	private class UIConnection extends Thread {

		private final Socket s;

		private final InputStream is;

		private final OutputStream os;

		private JAliEnCOMMander commander = null;

		/**
		 * One UI connection identified by the socket
		 *
		 * @param s
		 * @throws IOException
		 */
		public UIConnection(final Socket s) throws IOException {
			this.s = s;

			is = s.getInputStream();
			os = s.getOutputStream();

			setName("UIConnection: " + s.getInetAddress());
		}

		private void waitCommandFinish() {
			// wait for the previous command to finish

			if (commander == null)
				return;

			while (commander.status.get() == 1)
				try {
					synchronized (commander.status) {
						commander.status.wait(1000);
					}
				}
				catch (@SuppressWarnings("unused") final InterruptedException ie) {
					// ignore
				}
		}

		private UIPrintWriter out = null;

		private void setShellPrintWriter(final OutputStream os, final String shelltype) {
			if (shelltype.equals("jaliensh"))
				out = new JShPrintWriter(os);
			else
				out = new XMLPrintWriter(os);
		}

		@Override
		public void run() {
			connectedClients.incrementAndGet();

			notifyActivity();

			BufferedReader br = null;

			try {
				// String sCmdDebug = "";
				String sCmdValue = "";

				String sLine = "";

				// Get the DOM Builder Factory
				final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				// Get the DOM Builder
				final DocumentBuilder builder = factory.newDocumentBuilder();
				// Load and Parse the XML document
				// document contains the complete XML as a Tree.
				br = new BufferedReader(new InputStreamReader(is));
				String sCommand = "";

				while ((sLine = br.readLine()) != null)
					if (sLine.startsWith("<document>"))
						sCommand = sLine;
					else
						if (sLine.endsWith("</document>")) {
							sCommand += sLine;
							final ArrayList<String> cmdOptions = new ArrayList<>();
							final ArrayList<String> fullCmd = new ArrayList<>();
							try {

								// <document>
								// <ls>
								// <o>-l</o>
								// <o>-a</o>
								// <o>/alice/cern.ch/user/t/ttothova</o>
								// </ls>
								// </document>
								logger.info("XML =\"" + sCommand + "\"");
								final Document document = builder.parse(new InputSource(new StringReader(sCommand)));

								final NodeList commandNodeList = document.getElementsByTagName("command");

								if (commandNodeList != null && commandNodeList.getLength() == 1) {
									final Node commandNode = commandNodeList.item(0);
									sCmdValue = commandNode.getTextContent();
									fullCmd.add(sCmdValue);
									logger.info("Received command " + sCmdValue);

									final NodeList optionsNodeList = document.getElementsByTagName("o");

									for (int i = 0; i < optionsNodeList.getLength(); i++) {
										final Node optionNode = optionsNodeList.item(i);
										cmdOptions.add(optionNode.getTextContent());
										fullCmd.add(optionNode.getTextContent());
										logger.info("Command options = " + optionNode.getTextContent());
									}

									if (sCmdValue != null && sCmdValue.equals("password")) {

										if (cmdOptions.get(0).equals(password)) {
											os.write(passACK.getBytes());
											os.flush();
										}
										else {
											os.write(passNOACK.getBytes());
											os.flush();
											return;
										}
									}
									else {
										logger.log(Level.INFO, "JSh connected.");

										if (commander == null) {
											commander = new JAliEnCOMMander();
											commander.start();
										}

										notifyActivity();

										if ("SIGINT".equals(sLine)) {
											logger.log(Level.INFO, "Received [SIGINT] from JSh.");

											try {
												commander.interrupt();
												commander.kill = true;
											}
											catch (@SuppressWarnings("unused") final Throwable t) {
												// ignore
											}
											finally {
												System.out.println("SIGINT reset commander");

												// kill the active command and start a new instance
												final JAliEnCOMMander comm = new JAliEnCOMMander(commander.getUser(), commander.getCurrentDir(), commander.getSite(), out);
												commander = comm;

												commander.start();

												commander.flush();
											}
										}
										else
											if ("shutdown".equals(sLine))
												shutdown();
											else {
												waitCommandFinish();

												synchronized (commander) {
													if ("setshell".equals(sCmdValue) && cmdOptions.size() > 0) {
														setShellPrintWriter(os, cmdOptions.get(0));
														logger.log(Level.INFO, "Set explicit print writer: " + cmdOptions.get(0));

														os.write((JShPrintWriter.streamend + "\n").getBytes());
														os.flush();
														continue;
													}

													if (out == null)
														out = new XMLPrintWriter(os);

													commander.setLine(out, fullCmd.toArray(new String[0]));

													commander.notifyAll();
												}
											}
										os.flush();
									}
								}
								else
									logger.severe("Received more than one command");
								// some error, there was more than one command
								// attached to the document
							}
							catch (final Exception e) {
								logger.severe("Parse error " + e.getMessage());
							}
						}
						else
							sCommand += "\n" + sLine;

			}
			catch (final Throwable e) {
				logger.log(Level.INFO, "Error running the commander.", e);
			}
			finally {
				waitCommandFinish();

				if (br != null)
					try {
						br.close();
					}
					catch (@SuppressWarnings("unused") final IOException ioe) {
						// ignore
					}

				connectedClients.decrementAndGet();

				notifyActivity();

				try {
					s.shutdownOutput();
				}
				catch (@SuppressWarnings("unused") final Exception e) {
					// nothing particular
				}
				try {
					s.shutdownInput();
				}
				catch (@SuppressWarnings("unused") final Exception e) {
					// ignore
				}
				try {
					s.close();
				}
				catch (@SuppressWarnings("unused") final Exception e) {
					// ignore
				}
			}
		}

	}

	private transient boolean alive = true;

	/**
	 * Kill the JBox instance
	 */
	void shutdown() {

		logger.log(Level.FINE, "Received [shutdown] from JSh.");

		alive = false;

		try {
			ssocket.close();
		}
		catch (@SuppressWarnings("unused") final IOException e) {
			// ignore, we're dead anyway
		}

		if (shutdownThread != null)
			shutdownThread.interrupt();

		logger.log(Level.INFO, "JBox: We die gracefully...Bye!");
		System.exit(0);

	}

	@Override
	public void run() {
		while (alive)
			try {
				@SuppressWarnings("resource")
				final Socket s = ssocket.accept();

				connection = new UIConnection(s);

				connection.start();
			}
			catch (final Exception e) {
				if (alive)
					logger.log(Level.WARNING, "Cannot use socket: ", e.getMessage());
			}
	}

	/**
	 * Singleton
	 */
	static JBoxServer server = null;

	/**
	 * Start once the UIServer
	 *
	 * @param iDebugLevel
	 */
	public static synchronized void startJBoxServer(final int iDebugLevel) {

		logger.log(Level.INFO, "JBox starting ...");

		if (server != null)
			return;

		preempt();

		// Get port range from config
		final boolean portAny = ConfigUtils.getConfig().getb("port.range.any", true);

		final int portMin = Integer.parseInt(ConfigUtils.getConfig().gets("port.range.start", "10100"));
		final int portMax = Integer.parseInt(ConfigUtils.getConfig().gets("port.range.end", portAny ? String.valueOf(portMin + 1) : "10200"));

		for (int port = portMin; port < portMax; port++)
			try {
				server = new JBoxServer(portAny ? 0 : port, iDebugLevel);
				server.start();

				logger.log(Level.INFO, "JBox listening on port " + server.port);
				System.out.println("JBox is listening on port " + server.port);

				break;
			}
			catch (final Exception ioe) {
				// we don't need the already in use info on the port, maybe
				// there's another user on the machine...
				logger.log(Level.FINE, "JBox: Could not listen on port " + String.valueOf(portAny ? 0 : port), ioe);
			}
	}

	/**
	 *
	 * Load necessary keys and start JBoxServer
	 *
	 * @param iDebug
	 */
	public static void startJBoxService(final int iDebug) {
		logger.log(Level.INFO, "Starting JBox");
		System.err.println(passACK);
		JBoxServer.startJBoxServer(iDebug);
	}

	/**
	 *
	 * Get the port used by JBoxServer
	 *
	 * @return the TCP port this server is listening on. Can be negative to signal that the server is actually not listening on any port (yet?)
	 *
	 */
	public static int getPort() {
		return server != null ? server.port : -1;
	}

}
