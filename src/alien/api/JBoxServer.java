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
import java.util.Scanner;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
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
	static transient final Logger logger = ConfigUtils
	.getLogger(JBoxServer.class.getCanonicalName());

	// this triggers to ask for the user home LFN before doing anything else
	private static boolean preemptJCentralConnection=true;
	
	/**
	 * 
	 */
	public static final String passACK = "OKPASSACK";
	
	/**
	 * 
	 */
	public static final String passNOACK = "NOPASSACK";
	
	private final int port;

	private ServerSocket ssocket;
	
	private UIConnection connection;

	/**
	 * The password
	 */
	final String password;

	/**
	 * Debug level received from the user
	 * */
	final int iDebugLevel;

	private static synchronized void preempt(){
		if(preemptJCentralConnection){
			PreemptJCentralConnection preempt = new PreemptJCentralConnection();
			preempt.start();
			preemptJCentralConnection=false;
		}
	}
	
	/**
	 * Start the server on a given port
	 * 
	 * @param listeningPort
	 * @throws IOException
	 */
	private JBoxServer(final int listeningPort, int iDebug) throws Exception {
		preempt();
		
		this.port = listeningPort;
		this.iDebugLevel = iDebug;

		InetAddress localhost = InetAddress.getByName("127.0.0.1");

		ssocket = new ServerSocket(port, 10, localhost);

		password = UUID.randomUUID().toString();
		
		AliEnPrincipal alUser = AuthorizationFactory.getDefaultUser();
		
		if(alUser==null || alUser.getName()==null)
			throw new Exception("Could not get your username. FATAL!");
		
		//here we should get home directory
		String sHomeUser = UsersHelper.getHomeDir(alUser.getName());
		
		//should check if the file was written and if not then exit.
		if (!writeTokenFile("127.0.0.1", listeningPort, password, alUser.getName(), sHomeUser, this.iDebugLevel)){ //user should be taken from certificate
			throw new Exception("Could not write the token file! No application can connect to JBox");

		}

		if(!writeEnvFile("127.0.0.1", listeningPort, alUser.getName())){
			throw new Exception("Could not write the env file! JSh/JRoot will not be able to connect to JBox");
		}
	}

	/**
	 * write the configuration file that is used by gapi <br />
	 * the filename = /tmp/gclient_token_$uid
	 * @param sHost hostname to connect to, by default localhost
	 * @param iPort port number for listening
	 * @param sPassword the password used by other application to connect to the JBoxServer
	 * @param sUser the user from the certificate
	 * @param iDebug the debug level received from the command line
	 * @return true if the file was written, false if not
	 * @author Alina Grigoras
	 */
	private static boolean writeTokenFile(String sHost, int iPort, String sPassword, String sUser, String sHomeUser, int iDebug){
		String sUserId = System.getProperty("userid");

		if(sUserId == null || sUserId.length() == 0){
			logger.severe("User Id empty! Could not get the token file name");
			return false;
		}

		try {
			int iUserId = Integer.parseInt(sUserId);

			String sFileName = "/tmp/gclient_token_"+iUserId;

			try {
				FileWriter fw = new FileWriter(sFileName);

				fw.write("Host = "+sHost+"\n");
				logger.fine("Host = "+sHost);

				fw.write("Port = "+iPort+"\n");
				logger.fine("Port = "+iPort);

				fw.write("User = "+sUser+"\n");
				logger.fine("User = "+sUser);

				fw.write("Home = "+sHomeUser+"\n");
				logger.fine("Home = "+sHomeUser);
				
				fw.write("Passwd = "+sPassword+"\n");
				logger.fine("Passwd = "+sPassword);

				fw.write("Debug = "+iDebug+"\n");
				logger.fine("Debug = "+iDebug);
				
				fw.write("PID = "+MonitorFactory.getSelfProcessID() +"\n");
				logger.fine("PID = "+MonitorFactory.getSelfProcessID());

				fw.flush();
				fw.close();

				return true;

			} catch (Exception e1) {
				logger.severe("Could not open file "+sFileName+ " to write");
				logger.severe(e1.getMessage());
				
				e1.printStackTrace();
				return false;
			}
		} catch (Throwable e) {
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
	 * @param iPort
	 * @param sPassword
	 * @param sUser
	 * @return
	 */
	private static boolean writeEnvFile(String sHost, int iPort, String sUser){
		String sUserId = System.getProperty("userid");

		if(sUserId == null || sUserId.length() == 0){
			logger.severe("User Id empty! Could not get the env file name");
			return false;
		}

//		String sAlienRoot = System.getenv("ALIEN_ROOT");
//		
//		if(sAlienRoot == null || sAlienRoot.length() ==0 ){
//			logger.severe("No ALIEN_ROOT found. Please set ALIEN_ROOT environment variable");
//			System.out.println("You don't have $ALIEN_ROOT set. You will not be able to copy files.");
//		}
		
		try {
			int iUserId = Integer.parseInt(sUserId);

			String sFileName = "/tmp/gclient_env_"+iUserId;

			try {
				FileWriter fw = new FileWriter(sFileName);
				
				fw.write("export alien_API_HOST="+sHost+"\n");
				logger.fine("export alien_API_HOST="+sHost);

				fw.write("export alien_API_PORT="+iPort+"\n");
				logger.fine("export alien_API_PORT="+iPort);

				fw.write("export alien_API_USER="+sUser+"\n");
				logger.fine("export alien_API_USER="+sUser);
				
				fw.flush();
				fw.close();

				return true;

			} catch (Exception e1) {
				logger.severe("Could not open file "+sFileName+ " to write");
				e1.printStackTrace();
				return false;
			}
		} catch (Exception e) {
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
			new JAliEnCOMMander(null).getId();
		}
	}
	
	
	/**
	 * One UI connection
	 * 
	 * @author costing
	 */
	private class UIConnection extends Thread {
		
		private final Socket s;
		
		private final JBoxServer jbox;

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
			this.jbox = jbox;
			is = s.getInputStream();
			os = s.getOutputStream();

			setName("UIConnection: " + s.getInetAddress());
		}

		@Override
		public void run() {
				try {

					final String lineTerm = String.valueOf((char) 0);
					final String SpaceSep = String.valueOf((char) 1);

					BufferedReader br = new BufferedReader(
							new InputStreamReader(is));

					Scanner scanner = new Scanner(br);
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
					commander = new JAliEnCOMMander(jbox);

					while (scanner.hasNext()) {
						String line = scanner.next();
						if ("SIGINT".equals(line)){
							logger.log(Level.INFO, "Received [SIGINT] from JSh.");
							commander.killRunningCommand();
						}else if("shutdown".equals(line)){
							shutdown();
							
						}else {

							try {
								while (commander.isAlive()
										&& !commander.getState().equals(
												State.WAITING)) {
									Thread.sleep(1);
								}
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							commander.setLine(os, line.split(SpaceSep));

							
							try{
								if (!commander.isAlive()) 
								commander.start();
							}
							catch(IllegalThreadStateException e){
								//ignore
							}
							//if (!commander.isAlive()) {
							//	commander.start();
							//} else 
								if (commander.getState().equals(
									State.WAITING)) {
								synchronized (commander) {
									commander.notify();
								}
							}
						}
						os.flush();
					}
				} catch (Throwable e) {
					logger.log(Level.INFO, "Error running the commander.", e);
				} finally {
					try {
						s.shutdownOutput();
					} catch (Exception e) {
						// nothing particular
					}
					try {
						s.shutdownInput();
					} catch (Exception e) {
						// ignore
					}
					try {
						s.close();
					} catch (Exception e) {
						// ignore
					}
				}
		}
		
	}
	
	private transient boolean alive = true;
	
	private void shutdown(){
	
		logger.log(Level.FINE, "Received [shutdown] from JSh.");

		alive = false;
		
		try {
			ssocket.close();
		} catch (IOException e) {
			// ignore, we're dead anyway
		}
		logger.log(Level.INFO, "JBox: We die gracefully...Bye!");
		System.exit(0);
	
	}
	
	
	@Override
	public void run() {
		while (alive) {
			try {
				final Socket s = ssocket.accept();
				
				connection = new UIConnection(s,this);
				
				connection.start();
			}
			catch (Exception e) {
				if(alive)
					logger.log(Level.WARNING, "Cannot use socket: ", e.getMessage());
			}
		}
	}

	private static JBoxServer server = null;

	/**
	 * Start once the UIServer
	 * @param iDebugLevel 
	 */
	public static synchronized void startJBoxServer(int iDebugLevel) {

		logger.log(Level.INFO, "JBox starting ...");

		if (server != null)
			return;

		for (int port = 10100; port < 10200; port++) {
			try {
		
				server = new JBoxServer(port, iDebugLevel);
				server.start();

				logger.log(Level.INFO, "JBox listening on port " + port);
				System.out.println("JBox is listening...");
				
				break;
			} catch (Exception ioe) {
				// we don't need the already in use info on the port, maybe there's another user on the machine...
				logger.log(Level.FINE, "JBox: Could not listen on port " + port, ioe);
			}
		}	
	}

	/**
	 * 
	 * Load necessary keys and start JBoxServer
	 * @param iDebug 
	 */
	public static void startJBoxService(int iDebug) {
		logger.log(Level.INFO, "Starting JBox");
		try {
			if(!JAKeyStore.loadClientKeyStorage()){
				System.err.println("Grid Certificate could not be loaded.");
				System.err.println("Exiting...");
			}
			else {
				System.err.println(passACK);
				JBoxServer.startJBoxServer(iDebug);
			}
		} catch (org.bouncycastle.openssl.EncryptionException e) {
			logger.log(Level.SEVERE, "Wrong password!");
			System.err.println("Wrong password!");
		} catch (javax.crypto.BadPaddingException e) {
			logger.log(Level.SEVERE, "Wrong password!");
			System.err.println("Wrong password!");
		}

		catch (Exception e) {
			logger.log(Level.SEVERE, "Error loading the key");
			System.err.println("Error loading the key");
			e.printStackTrace();
		}
	}
	
}
