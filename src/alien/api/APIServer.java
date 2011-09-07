package alien.api;

import java.io.BufferedReader;
import java.io.File;
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

/**
 * Simple UI server to be used by ROOT and command line
 * 
 * @author costing
 */
public class APIServer extends Thread {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(APIServer.class.getCanonicalName());

	private final int port;

	private ServerSocket ssocket;

	/**
	 * The password
	 */
	final String password;

	/**
	 * Debug level received from the user
	 * */
	final int iDebugLevel;
	
	/**
	 * Start the server on a given port
	 * 
	 * @param listeningPort
	 * @throws IOException
	 */
	private APIServer(final int listeningPort, int iDebug) throws Exception {
		this.port = listeningPort;
		this.iDebugLevel = iDebug;

		InetAddress localhost = InetAddress.getByName("127.0.0.1");

		System.err.println("Trying to bind to " + localhost + " : " + port);

		ssocket = new ServerSocket(port, 10, localhost);

		password = UUID.randomUUID().toString();

		//should check if the file was written and if not then exit.
		if (!writeTokenFile(listeningPort, password, "agrigora", this.iDebugLevel)){ //user should be taken from certificate
			throw new Exception("Could not write the token file! No application can connect to the APIServer");
			
		}
		
		final File fHome = new File(System.getProperty("user.home"));

		final File f = new File(fHome, ".alien");

		f.mkdirs();

		FileWriter fw;
		try {
			fw = new FileWriter(new File(f, ".uisession"));

			fw.write("127.0.0.1:" + port + "\n" + password + "\n"
					+ MonitorFactory.getSelfProcessID() + "\n");
			fw.flush();
			fw.close();
		} catch (IOException e) {
			ssocket.close();

			throw e;
		}
		
		
	}

	/**
	 * write the configuration file that is used by the gapi <br />
	 * the filename = /tmp/jclient_token_$uid
	 * @param iPort port number for listening
	 * @param sPassword the password used by other application to connect to the APIServer
	 * @param sUser the user from the certificate
	 * @param iDebug the debug level received from the command line
	 * @return true if the file was written, false if not
	 */
	private boolean writeTokenFile(int iPort, String sPassword, String sUser, int iDebug){
		String sUserId = System.getProperty("userid");
		
		if(sUserId == null || sUserId.length() == 0){
			logger.severe("User Id empty! Could not get the token file name");
			return false;
		}
		
		try {
			int iUserId = Integer.parseInt(sUserId);
			
			String sFileName = "/tmp/jclient_token_"+iUserId;
			
			try {
				FileWriter fw = new FileWriter(sFileName);
				
				fw.write("Host = 127.0.0.1\n");
				logger.fine("Host = 127.0.0.1");
				
				fw.write("Port = "+iPort+"\n");
				logger.fine("Port = "+iPort);
				
				AliEnPrincipal alUser = AuthorizationFactory.getDefaultUser();
	
				fw.write("User = "+alUser.getName()+"\n");
				logger.fine("User = "+alUser.getName());
				
				fw.write("Passwd = "+sPassword+"\n");
				logger.fine("Passwd = "+sPassword);
				
				fw.write("Debug = "+iDebug+"\n");
				logger.fine("Debug = "+iDebug);
				
				fw.flush();
				fw.close();
				
				return true;
				
			} catch (Exception e1) {
				logger.severe("Could not open file "+sFileName+ " to write");
				e1.printStackTrace();
				return false;
			}
		} catch (Exception e) {
			logger.severe("Could not get user id! The token file could not be created ");
			e.printStackTrace();
			return false;
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

		@Override
		public void run() {
			try {
				
				BufferedReader br = new BufferedReader(
						new InputStreamReader(is));

				Scanner scanner = new Scanner(br);
		        scanner.useDelimiter(String.valueOf((char) 0));
		        
				String sLine = br.readLine();
				//String sLine = scanner.next();

				if (sLine != null && sLine.equals(password)) {
					System.out.println("password accepted");
					os.write("OKPASSACK".getBytes());
					os.flush();
				} else {
					os.write("NOPASSACK".getBytes());
					os.flush();
					return;
				}
				JAliEnCOMMander jcomm = new JAliEnCOMMander();

		        while (scanner.hasNext()) {
		        	String line = scanner.next();
		        	System.out.println("WE received call: " + line);
		        	jcomm.execute(os, line.split(String.valueOf((char) 1)));
					os.flush();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					s.shutdownOutput();
				} catch (IOException e) {
					// nothing particular
				}
				try {
					s.shutdownInput();
				} catch (IOException e) {
					// ignore
				}
				try {
					s.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}
	}

	@Override
	public void run() {
		while (true) {
			try {
				final Socket s = ssocket.accept();

				final UIConnection conn = new UIConnection(s);

				conn.start();
			} catch (IOException e) {
				continue;
			}
		}
	}

	private static APIServer server = null;

	/**
	 * Start once the UIServer
	 */
	private static synchronized void startAPIServer(int iDebugLevel) {
		if (server != null)
			return;

		for (int port = 10100; port < 10200; port++) {
			try {
				server = new APIServer(port, iDebugLevel);
				server.start();

				logger.log(Level.INFO, "UIServer listening on port " + port);
				System.err.println("Listening on " + port);

				break;
			} catch (Exception ioe) {
				System.err.println(ioe);
				ioe.printStackTrace();
				logger.log(Level.FINE, "Could not listen on port " + port, ioe);
			}
		}
	}

	/**
	 * 
	 * Load necessary keys and start APIServer
	 */
	public static void startAPIService(int iDebug) {
		try {
			JAKeyStore.loadClientKeyStorage();
			APIServer.startAPIServer(iDebug);
		} catch (org.bouncycastle.openssl.EncryptionException e) {
			System.err.println("Wrong password!");
		} catch (javax.crypto.BadPaddingException e) {
			System.err.println("Wrong password!");
		}

		catch (Exception e) {
			System.err.println("Error loading the key");
			e.printStackTrace();
		}

	}

}
