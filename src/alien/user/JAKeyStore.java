package alien.user;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import lazyj.ExtProperties;
import lazyj.Utils;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;
import org.bouncycastle.openssl.PasswordFinder;

import alien.catalogue.CatalogueUtils;
import alien.config.ConfigUtils;

/**
 * 
 * @author ron
 * @since Jun 22, 2011
 */
public class JAKeyStore {

	

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(CatalogueUtils.class.getCanonicalName());
	
	
	/**
	 * length for the password generator
	 */
	private static final int passLength = 30;
	
	/**
	 * 
	 */
	public static KeyStore clientCert = null;
	
	/**
	 * 
	 */
	public static KeyStore hostCert = null;

	/**
	 * 
	 */
	public static KeyStore trustStore;
	
	/**
	 * 
	 */
	public static X509Certificate[] trustedCertificates;

	/**
	 * 
	 */
	public static char[] pass = getRandomString();
	

	/**
	 * 
	 */
	public static TrustManager trusts[];

	static {
		Security.addProvider(new BouncyCastleProvider());
	}


	private static void loadTrusts() {
		try {
			trustStore = KeyStore.getInstance("JKS");

			trustStore.load(null, pass);

			TrustManagerFactory tmf;

			tmf = TrustManagerFactory.getInstance("SunX509");

			File trustsDir = new File(ConfigUtils.getConfig().gets(
					"trusted.certificates.location",
					System.getProperty("user.home")
							+ System.getProperty("file.separator") + ".j"
							+ System.getProperty("file.separator") + "trusts"));

			if (trustsDir.exists() && trustsDir.isDirectory()) {
				CertificateFactory cf;

				cf = CertificateFactory.getInstance("X.509");

				for (final File trust : trustsDir.listFiles()) {

					if (trust.getName().endsWith("der")) {
						FileInputStream fis = null;
						
						try {
							fis = new FileInputStream(trust);
							
							X509Certificate c = (X509Certificate) cf.generateCertificate(fis);
							if (logger.isLoggable(Level.INFO)) {
								logger.log(Level.INFO,"Trusting now: " + c.getSubjectDN());
							}

							trustStore.setEntry(trust.getName().substring(0,trust.getName().indexOf(".der")), new KeyStore.TrustedCertificateEntry(c), null);
							
							if (hostCert != null)
								hostCert.setEntry(trust.getName().substring(0, trust.getName().indexOf(".der")), new KeyStore.TrustedCertificateEntry(c), null);
							
							if (clientCert != null)
								clientCert.setEntry(trust.getName().substring(0, trust.getName().indexOf(".der")), new KeyStore.TrustedCertificateEntry(c), null);

						}
						catch (final Exception e) {
							e.printStackTrace();
						}
						finally{
							if (fis!=null){
								try{
									fis.close();
								}
								catch (final IOException ioe){
									// ignore
								}
							}
						}
					}
				}
			} else{
				if (logger.isLoggable(Level.SEVERE)) {
					logger.log(Level.SEVERE,"Found no trusts to load in: " + trustsDir);
				}
				System.err.println("Found no trusts to load in: " + trustsDir);

			}

			tmf.init(trustStore);
			trusts = tmf.getTrustManagers();

		}
		catch (IOException e) {
			// TODO : print some message here 
		}
		catch (KeyStoreException e) {
			// TODO : print some message here
		}
		catch (CertificateException e) {
			// TODO : print some message here
		}
		catch (NoSuchAlgorithmException e) {
			// ignore
		}
	}

	private static boolean checkKeyPermissions(final String user_key,
			final String user_cert) {
		File key = new File(user_key);

		try {
			if (!user_key.equals(key.getCanonicalPath()))
				key = new File(key.getCanonicalPath());

			if (key.exists() && key.canRead()) {
				CommandOutput co = SystemCommand.bash(
						"ls -la " + key.getCanonicalPath(), false);

				if (!co.stdout.startsWith("-r--------")) {
					System.out.println("key|" + co.stdout + "|");
					changeMod("key", key, 400);

					co = SystemCommand.bash("ls -la " + user_key, false);

					if (!co.stdout.startsWith("-r--------"))
						return false;
				}
			} else
				return false;
		} catch (IOException e) {
			System.err.println("Error reading key file [" + user_key + "].");
		}

		File cert = new File(user_cert);

		try {
			if (!user_cert.equals(cert.getCanonicalPath()))
				cert = new File(cert.getCanonicalPath());

			if (cert.exists() && cert.canRead()) {
				CommandOutput co = SystemCommand.bash(
						"ls -la " + cert.getCanonicalPath(), false);

				if (!co.stdout.startsWith("-r--r-----")) {
					System.out.println("cert|" + co.stdout + "|");
					changeMod("certificate", cert, 440);

					co = SystemCommand.bash("ls -la " + user_cert, false);

					return co.stdout.startsWith("-r--r-----");
				}

				return true;
			}

			return false;
		} catch (IOException e) {
			System.err.println("Error reading cert file [" + user_cert + "].");
		}

		return false;
	}

	private static boolean changeMod(final String name, final File file, final int chmod) {
		try {
			String ack="";

			final Console cons = System.console();
			
			if (cons==null)
				return false;
			
			System.out.println("Your Grid " + name + " file has wrong permissions.");
			System.out.println("The file [ " + file.getCanonicalPath() + " ] should have permissions [ " + chmod + " ].");
			
			if ((ack = cons.readLine("%s", "Would you correct this now [Yes/no]?")) != null)
				if (Utils.stringToBool(ack, true)) {
					final CommandOutput co = SystemCommand.bash("chmod " + chmod + " " + file.getCanonicalPath(), false);

					if (co.exitCode!=0){
						System.err.println("Could not change permissions: "+co.stderr);
					}
					
					return co.exitCode == 0;
				}
		} 
		catch (IOException e) {
			//ignore
		}
		
		return false;
	}
	
	
	/**
	 * @return  true if ok
	 * @throws Exception
	 */
	public static boolean loadClientKeyStorage() throws Exception {
		return loadClientKeyStorage(false);
	}
	
	/**
	 * @param noUserPass 
	 * @return true if ok
	 * @throws Exception
	 */
	public static boolean loadClientKeyStorage(final boolean noUserPass) throws Exception {

		ExtProperties config = ConfigUtils.getConfig();
		
		String user_key = config.gets(
				"user.cert.priv.location",
				System.getProperty("user.home")
						+ System.getProperty("file.separator")
						+ ".globus"
						+ System.getProperty("file.separator")
						+ "userkey.pem");
		
		String user_cert = config.gets(
				"user.cert.pub.location",
				System.getProperty("user.home")
						+ System.getProperty("file.separator")
						+ ".globus"
						+ System.getProperty("file.separator")
						+ "usercert.pem");
		
		if(!checkKeyPermissions(user_key, user_cert))
			return false;
		
		clientCert = KeyStore.getInstance("JKS");

		try {
			clientCert.load(null, pass);
		} catch (Exception e) {
			//ignore
		} 
		
		JPasswordFinder jpf;
		
		if(noUserPass)
			jpf =  new JPasswordFinder(new char[]{});
		else
			jpf =  getPassword();
		
		addKeyPairToKeyStore(
				clientCert,
				"User.cert",
				user_key,
				user_cert,
				jpf
				);

		loadTrusts();
		return true;

	}

	/**
	 * @throws Exception
	 */
	public static void loadPilotKeyStorage() throws Exception {

		ExtProperties config = ConfigUtils.getConfig();

		clientCert = KeyStore.getInstance("JKS");

		try {
			//pass = getRandomString();

			clientCert.load(null, pass);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (CertificateException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		addKeyPairToKeyStore(
				clientCert,
				"User.cert",
				config.gets(
						"host.cert.priv.location",
						System.getProperty("user.home")
								+ System.getProperty("file.separator")
								+ ".globus"
								+ System.getProperty("file.separator")
								+ "hostkey.pem"),
				config.gets(
						"host.cert.pub.location",
						System.getProperty("user.home")
								+ System.getProperty("file.separator")
								+ ".globus"
								+ System.getProperty("file.separator")
								+ "hostcert.pem"), new JPasswordFinder(new char[]{}));

		
		loadTrusts();
	}

	/**
	 * @throws Exception
	 */
	public static void loadServerKeyStorage() throws Exception {

		ExtProperties config = ConfigUtils.getConfig();
		//pass = getRandomString();
		
		String hostkey = config.gets(
				"host.cert.priv.location",
				System.getProperty("user.home")
						+ System.getProperty("file.separator")
						+ ".globus"
						+ System.getProperty("file.separator")
						+ "hostkey.pem");

		String hostcert = config.gets(
				"host.cert.pub.location",
				System.getProperty("user.home")
						+ System.getProperty("file.separator")
						+ ".globus"
						+ System.getProperty("file.separator")
						+ "hostcert.pem");
		
		hostCert = KeyStore.getInstance("JKS");
		hostCert.load(null, pass);
			
		addKeyPairToKeyStore(
				hostCert,
				"Host.cert",
				hostkey,
				hostcert, null);

		loadTrusts();

	}

	private static JPasswordFinder getPassword() {

//		Console cons;
//		char[] passwd;
		
		
//		if ((cons = System.console()) == null)
//			System.err
//					.println("Could not get console to request key password.");
//		if (logger.isLoggable(Level.SEVERE)) {
//			logger.log(Level.SEVERE, "Could not get console to request key password.");
//		}
//
//		if ((cons = System.console()) != null
//				&& (passwd = cons.readPassword("[%s]", consoleMessage
//						+ " password: ")) != null)
//			password = String.valueOf(passwd);

	    final BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	    
	    try {
	    	final String line = in.readLine();
	    	
	    	if (line!=null && line.length()>0)
	    		return new JPasswordFinder(line.toCharArray());
		}
	    catch (final IOException e) {
			logger.log(Level.INFO,"Could not read passwd from System.in .");
		}
	    
	    return new JPasswordFinder("".toCharArray());
		
	}

	@SuppressWarnings("unused")
	private static void createKeyStore(KeyStore ks, String keyStoreName) {

		//pass  = getRandomString();

		FileInputStream f = null;
		try {
			try {
				f = new FileInputStream(keyStoreName);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			try {
				ks.load(null, pass);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (CertificateException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

		} finally {
			if (f != null) {
				try {
					f.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}

	}

	private static void addKeyPairToKeyStore(KeyStore ks, String entryBaseName,
			String privKeyLocation, String pubKeyLocation,
			PasswordFinder pFinder) throws Exception {

		ks.setEntry(
				entryBaseName,
				new KeyStore.PrivateKeyEntry(loadPrivX509(privKeyLocation,
						pFinder), loadPubX509(pubKeyLocation)),
				new KeyStore.PasswordProtection(pass));
	}

	@SuppressWarnings("unused")
	private static void saveKeyStore(KeyStore ks, String filename,
			char[] password) {

		FileOutputStream fo = null;
		try {
			try {
				fo = new FileOutputStream(filename);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			try {
				ks.store(fo, password);
			} catch (KeyStoreException e) {
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (CertificateException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} finally {
			if (fo != null) {
				try {
					fo.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}
	}

	/**
	 * @param keyFileLocation
	 * @param pFinder
	 * @return priv key
	 * @throws Exception
	 */
	public static PrivateKey loadPrivX509(String keyFileLocation,
			PasswordFinder pFinder) throws Exception {

		if (logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO,"Loading private key ... " + keyFileLocation);
		}
		
		BufferedReader priv = null;
		
		PEMReader reader = null;
		
		try{
			priv = new BufferedReader(new FileReader(keyFileLocation));

			if (pFinder == null)
				reader = new PEMReader(priv);
			else
				reader = new PEMReader(priv, pFinder);
			
			Object obj;
			while ( (obj=reader.readObject())!=null ){
				System.err.println(obj.getClass().getCanonicalName());
				
				if (obj instanceof PrivateKey)
					return (PrivateKey) obj;
				else
				if (obj instanceof KeyPair)
					return ((KeyPair) obj).getPrivate();
			}
			
			return null;
		}
		finally{
			if (reader!=null){
				try{
					reader.close();
				}
				catch (IOException ioe){
					// ignore
				}
			}
			
			if (priv!=null)
				priv.close();
		}
	}

	/**
	 * @param certFileLocation
	 * @return Cert chain
	 * @throws IOException
	 */
	public static Certificate[] loadPubX509(String certFileLocation)
			throws IOException {

		if (logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO,"Loading public ... " + certFileLocation);
		}

		BufferedReader pub = null;
		
		PEMReader reader = null;
		
		try {
			pub = new BufferedReader(new FileReader(certFileLocation));
			
			reader = new PEMReader(pub);
			
			return new Certificate[] { (Certificate) reader.readObject() };
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			if (reader!=null)
				reader.close();
			
			if (pub!=null)
				pub.close();
		}
		
		return null;
	}
	

	private static class JPasswordFinder implements PasswordFinder {

		private final char[] password;

		JPasswordFinder(char[] password) {
			this.password = password;
		}

		@Override
		public char[] getPassword() {
			return Arrays.copyOf(password, password.length);
		}
	}
	
    private static final String charString = "!0123456789abcdefghijklmnopqrstuvwxyz@#$%^&*()-+=_{}[]:;|?/>.,<";
	
    
    /**
     * @return randomized char array of passLength length
     */
    public static char[] getRandomString() {
        final Random ran = new Random(System.currentTimeMillis());
        StringBuffer s = new StringBuffer();
        for (int i = 0; i < passLength; i++) {
            int pos = ran.nextInt(charString.length());
            
            s.append(charString.charAt(pos));
        }
        return s.toString().toCharArray();
    }

}
