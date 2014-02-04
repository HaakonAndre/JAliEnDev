package alien.api;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JShPrintWriter;
import alien.shell.commands.RootPrintWriter;
import alien.shell.commands.UIPrintWriter;
import alien.user.AliEnPrincipal;
import alien.user.JAKeyStore;
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
	 * */
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

	static {
		new Thread() {
			@Override
			public void run() {
				while (true) {
					if (connectedClients.get() == 0 && (System.currentTimeMillis() - lastOperation) > 1000 * 60 * 60 * 24 && server != null)
						server.shutdown();

					try {
						Thread.sleep(1000 * 60);
					} catch (final InterruptedException ie) {
						// ignore
					}
				}
			}
		}.start();
	}

	private static synchronized void preempt() {
		if (preemptJCentralConnection) {
			final PreemptJCentralConnection preempt = new PreemptJCentralConnection();
			preempt.start();
			preemptJCentralConnection = false;
		}
	}

	/**
	 * Start the server on a given port
	 * 
	 * @param listeningPort
	 * @throws IOException
	 */
	private JBoxServer(final int listeningPort, final int iDebug) throws Exception {
		this.port = listeningPort;
		this.iDebugLevel = iDebug;

		final InetAddress localhost = InetAddress.getByName("127.0.0.1");

		ssocket = new ServerSocket(port, 10, localhost);

		password = UUID.randomUUID().toString();

		final AliEnPrincipal alUser = AuthorizationFactory.getDefaultUser();

		if (alUser == null || alUser.getName() == null)
			throw new Exception("Could not get your username. FATAL!");

		// here we should get home directory
		final String sHomeUser = UsersHelper.getHomeDir(alUser.getName());

		// should check if the file was written and if not then exit.
		if (!writeTokenFile("127.0.0.1", listeningPort, password, alUser.getName(), sHomeUser, this.iDebugLevel))
			throw new Exception("Could not write the token file! No application can connect to JBox");

		if (!writeEnvFile("127.0.0.1", listeningPort, alUser.getName()))
			throw new Exception("Could not write the env file! JSh/JRoot will not be able to connect to JBox");
	}

	/**
	 * write the configuration file that is used by gapi <br />
	 * the filename = /tmp/gclient_token_$uid
	 * 
	 * @param sHost
	 *            hostname to connect to, by default localhost
	 * @param iPort
	 *            port number for listening
	 * @param sPassword
	 *            the password used by other application to connect to the
	 *            JBoxServer
	 * @param sUser
	 *            the user from the certificate
	 * @param iDebug
	 *            the debug level received from the command line
	 * @return true if the file was written, false if not
	 * @author Alina Grigoras
	 */
	private static boolean writeTokenFile(final String sHost, final int iPort, final String sPassword, final String sUser, final String sHomeUser, final int iDebug) {
		final String sUserId = System.getProperty("userid");

		if (sUserId == null || sUserId.length() == 0) {
			logger.severe("User Id empty! Could not get the token file name");
			return false;
		}

		try {
			final int iUserId = Integer.parseInt(sUserId);

			final String sFileName = "/tmp/gclient_token_" + iUserId;

			try {
				final FileWriter fw = new FileWriter(sFileName);

				fw.write("Host = " + sHost + "\n");
				logger.fine("Host = " + sHost);

				fw.write("Port = " + iPort + "\n");
				logger.fine("Port = " + iPort);

				fw.write("User = " + sUser + "\n");
				logger.fine("User = " + sUser);

				fw.write("Home = " + sHomeUser + "\n");
				logger.fine("Home = " + sHomeUser);

				fw.write("Passwd = " + sPassword + "\n");
				logger.fine("Passwd = " + sPassword);

				fw.write("Debug = " + iDebug + "\n");
				logger.fine("Debug = " + iDebug);

				fw.write("PID = " + MonitorFactory.getSelfProcessID() + "\n");
				logger.fine("PID = " + MonitorFactory.getSelfProcessID());

				fw.flush();
				fw.close();

				return true;

			} catch (final Exception e1) {
				logger.severe("Could not open file " + sFileName + " to write");
				logger.severe(e1.getMessage());

				e1.printStackTrace();
				return false;
			}
		} catch (final Throwable e) {
			logger.severe("Could not get user id! The token file could not be created ");
			logger.severe(e.getMessage());
			e.printStackTrace();

			return false;
		}
	}

	/**
	 * Writes the environment file used by ROOT <br />
	 * It needs to ne named /tmp/gclient_env_$UID and to contain:
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
			final int iUserId = Integer.parseInt(sUserId);

			final String sFileName = "/tmp/gclient_env_" + iUserId;

			try {
				final FileWriter fw = new FileWriter(sFileName);

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

			} catch (final Exception e1) {
				logger.severe("Could not open file " + sFileName + " to write");
				e1.printStackTrace();
				return false;
			}
		} catch (final Exception e) {
			logger.severe("Could not get user id! The env file could not be created ");
			e.printStackTrace();
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
			SEUtils.getSE(0);
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

		private JAliEnCOMMander commander;

		/**
		 * One UI connection identified by the socket
		 * 
		 * @param s
		 * @param jbox
		 * @throws IOException
		 */
		public UIConnection(final Socket s, final JBoxServer jbox) throws IOException {
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
				} catch (final InterruptedException ie) {
					// ignore
				}
		}

		private UIPrintWriter out = null;

		private void setShellPrintWriter(final OutputStream os, final String shelltype) {
			if (shelltype.equals("jaliensh"))
				out = new JShPrintWriter(os);
			else
				out = new RootPrintWriter(os);
		}

		@Override
		public void run() {
			connectedClients.incrementAndGet();

			notifyActivity();

			BufferedReader br = null;
			Scanner scanner = null;

			try {

				final String lineTerm = String.valueOf((char) 0);
				final String SpaceSep = String.valueOf((char) 1);

				br = new BufferedReader(new InputStreamReader(is));

				scanner = new Scanner(br);
				scanner.useDelimiter(lineTerm);

				String sLine = null;
				if (scanner.hasNext())
					sLine = scanner.next();

				if (sLine != null && sLine.equals(password)) {
					os.write(passACK.getBytes());
					os.flush();
				} else {
					os.write(passNOACK.getBytes());
					os.flush();
					return;
				}
				logger.log(Level.INFO, "JSh connected.");

				commander = new JAliEnCOMMander();
				commander.start();

				while (scanner.hasNext()) {
					final String line = scanner.next();

					if (line == null || line.isEmpty()) {
						logger.log(Level.INFO, "Received emtpy line, nothing to do.");
						// os.write(JShPrintWriter.streamend.getBytes());
						// os.flush();
						continue;
					}

					logger.log(Level.INFO, "JBox Got line: " + line);

					notifyActivity();

					if ("SIGINT".equals(line)) {
						logger.log(Level.INFO, "Received [SIGINT] from JSh.");

						try {
							commander.interrupt();
							commander.stop();
						} catch (final Throwable t) {
							// ignore
						} finally {

							System.out.println("SIGINT reset commander");

							final JAliEnCOMMander comm = new JAliEnCOMMander(commander.getUser(), commander.getRole(), commander.getCurrentDir(), commander.getSite(), out); // kill
																																												// the
																																												// active
																																												// command
																																												// and
																																												// start
																																												// a
																																												// new
																																												// one
							commander = comm;

							commander.start();

							commander.flush();

						}

					} else if ("shutdown".equals(line))
						shutdown();
					else {
						waitCommandFinish();

						synchronized (commander) {

							final StringTokenizer t = new StringTokenizer(line, SpaceSep);
							final List<String> args = new ArrayList<>();
							while (t.hasMoreTokens())
								args.add(t.nextToken());

							if ("setshell".equals(args.get(0))) {
								setShellPrintWriter(os, args.get(1));
								logger.log(Level.INFO, "Set explicit print writer.");

								os.write((JShPrintWriter.streamend + "\n").getBytes());
								os.flush();
								continue;
							}

							if (out == null)
								out = new RootPrintWriter(os);

							commander.setLine(out, args.toArray(new String[] {}));

							commander.notifyAll();
						}
					}
					os.flush();
				}
			} catch (final Throwable e) {
				logger.log(Level.INFO, "Error running the commander.", e);
			} finally {
				waitCommandFinish();

				if (scanner != null)
					scanner.close();

				if (br != null)
					try {
						br.close();
					} catch (final IOException ioe) {
						// ignore
					}

				connectedClients.decrementAndGet();

				notifyActivity();

				try {
					s.shutdownOutput();
				} catch (final Exception e) {
					// nothing particular
				}
				try {
					s.shutdownInput();
				} catch (final Exception e) {
					// ignore
				}
				try {
					s.close();
				} catch (final Exception e) {
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
		} catch (final IOException e) {
			// ignore, we're dead anyway
		}
		logger.log(Level.INFO, "JBox: We die gracefully...Bye!");
		System.exit(0);

	}

	@Override
	public void run() {
		while (alive)
			try {
				@SuppressWarnings("resource")
				final Socket s = ssocket.accept();

				connection = new UIConnection(s, this);

				connection.start();
			} catch (final Exception e) {
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

		for (int port = 10100; port < 10200; port++)
			try {
				server = new JBoxServer(port, iDebugLevel);
				server.start();

				logger.log(Level.INFO, "JBox listening on port " + port);
				System.out.println("JBox is listening...");

				break;
			} catch (final Exception ioe) {
				// we don't need the already in use info on the port, maybe
				// there's another user on the machine...
				logger.log(Level.FINE, "JBox: Could not listen on port " + port, ioe);
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
		try {
			if (!JAKeyStore.loadClientKeyStorage()) {
				System.err.println("Grid Certificate could not be loaded.");
				System.err.println("Exiting...");
			} else {
				System.err.println(passACK);
				JBoxServer.startJBoxServer(iDebug);
			}
		} catch (final org.bouncycastle.openssl.EncryptionException e) {
			logger.log(Level.SEVERE, "Wrong password!");
			System.err.println("Wrong password!");
		} catch (final javax.crypto.BadPaddingException e) {
			logger.log(Level.SEVERE, "Wrong password!");
			System.err.println("Wrong password!");
		}

		catch (final Exception e) {
			logger.log(Level.SEVERE, "Error loading the key");
			System.err.println("Error loading the key");
			e.printStackTrace();
		}
	}

}
