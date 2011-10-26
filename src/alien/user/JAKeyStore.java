package alien.user;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
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

	//	loadTrustedCertificates();

	}

//	private static void loadTrustedCertificates() {
//		try {
//
//			File trustsDir = new File(ConfigUtils.getConfig().gets(
//					"trusted.certificates.location",
//					System.getProperty("user.home")
//							+ System.getProperty("file.separator") + ".alien"
//							+ System.getProperty("file.separator") + "trusted"));
//
//			if (trustsDir.exists() && trustsDir.isDirectory()) {
//				CertificateFactory cf;
//
//				cf = CertificateFactory.getInstance("X.509");
//
//				trustedCertificates = new X509Certificate[trustsDir.listFiles().length+1];
//				int ccount = 1;
//				
//				for (File trust : trustsDir.listFiles()) {
//
//					if (trust.getName().endsWith("der")) {
//
//						try {
//							X509Certificate c = (X509Certificate) cf
//									.generateCertificate(new FileInputStream(
//											trust));
//							System.out.println("Trusting now: "
//									+ c.getSubjectDN());
//							trustedCertificates[ccount] = c;
//							ccount++;
//
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//					}
//				}
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}

	private static void loadTrusts() {
		try {
			trustStore = KeyStore.getInstance("JKS");

			trustStore.load(null, pass);

			TrustManagerFactory tmf;

			tmf = TrustManagerFactory.getInstance("SunX509");

			File trustsDir = new File(ConfigUtils.getConfig().gets(
					"trusted.certificates.location",
					System.getProperty("user.home")
							+ System.getProperty("file.separator") + ".alien"
							+ System.getProperty("file.separator") + "trusted"));

			if (trustsDir.exists() && trustsDir.isDirectory()) {
				CertificateFactory cf;

				cf = CertificateFactory.getInstance("X.509");

				for (File trust : trustsDir.listFiles()) {

					if (trust.getName().endsWith("der")) {

						try {

							X509Certificate c = (X509Certificate) cf
									.generateCertificate(new FileInputStream(
											trust));
							if (logger.isLoggable(Level.INFO)) {
								logger.log(Level.INFO,"Trusting now: "
										+ c.getSubjectDN());
							}
							
							trustStore.setEntry(
									trust.getName().substring(0,
											trust.getName().indexOf(".der")),

									new KeyStore.TrustedCertificateEntry(c),
									null);
							if (hostCert != null)
								hostCert.setEntry(
										trust.getName()
												.substring(
														0,
														trust.getName()
																.indexOf(".der")),

										new KeyStore.TrustedCertificateEntry(c),
										null);
							if (clientCert != null)
								clientCert
										.setEntry(
												trust.getName()
														.substring(
																0,
																trust.getName()
																		.indexOf(
																				".der")),

												new KeyStore.TrustedCertificateEntry(
														c), null);

						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}

			tmf.init(trustStore);
			trusts = tmf.getTrustManagers();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @throws Exception
	 */
	public static void loadClientKeyStorage() throws Exception {
		loadClientKeyStorage(false);
	}
	
	/**
	 * @param noUserPass 
	 * @throws Exception
	 */
	public static void loadClientKeyStorage(final boolean noUserPass) throws Exception {

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
		
		JPasswordFinder jpf;
		
		if(noUserPass)
			jpf =  new JPasswordFinder(new char[]{});
		else
			jpf =  getPassword("Grid certificate");
		
		addKeyPairToKeyStore(
				clientCert,
				"User.cert",
				config.gets(
						"user.cert.priv.location",
						System.getProperty("user.home")
								+ System.getProperty("file.separator")
								+ ".globus"
								+ System.getProperty("file.separator")
								+ "userkey.pem"),
				config.gets(
						"user.cert.pub.location",
						System.getProperty("user.home")
								+ System.getProperty("file.separator")
								+ ".globus"
								+ System.getProperty("file.separator")
								+ "usercert.pem"),
								jpf
				);

		loadTrusts();

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
								+ "hostcert.pem"), null);

		
		loadTrusts();
	}

	/**
	 * @throws Exception
	 */
	public static void loadServerKeyStorage() throws Exception {

		ExtProperties config = ConfigUtils.getConfig();
		//pass = getRandomString();

		hostCert = KeyStore.getInstance("JKS");
		hostCert.load(null, pass);
		
		addKeyPairToKeyStore(
				hostCert,
				"Host.cert",
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
								+ "globus"
								+ System.getProperty("file.separator")
								+ "hostcert.pem"), null);

		loadTrusts();

	}

	private static JPasswordFinder getPassword(String consoleMessage) {

		String pass = "";
		Console cons;
		char[] passwd = new char[] {};
		if ((cons = System.console()) == null)
			System.err
					.println("Could not get console to request key password.");
		if (logger.isLoggable(Level.SEVERE)) {
			logger.log(Level.SEVERE, "Could not get console to request key password.");
		}

		if ((cons = System.console()) != null
				&& (passwd = cons.readPassword("[%s]", consoleMessage
						+ " password: ")) != null)
			pass = String.valueOf(passwd);

		// System.out.println("pass is: " + pass);
		return new JPasswordFinder(pass.toCharArray());
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
		BufferedReader priv = new BufferedReader(
				new FileReader(keyFileLocation));

		if (pFinder == null)
			return ((KeyPair) new PEMReader(priv).readObject()).getPrivate();

		return ((KeyPair) new PEMReader(priv, pFinder).readObject())
				.getPrivate();

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

		BufferedReader pub = new BufferedReader(new FileReader(certFileLocation));
		try {
			return new Certificate[] { (Certificate) new PEMReader(pub)
					.readObject() };

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	

	private static class JPasswordFinder implements PasswordFinder {

		private final char[] password;

		private JPasswordFinder(char[] password) {
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
        Random ran = new Random(System.currentTimeMillis());
        StringBuffer s = new StringBuffer();
        for (int i = 0; i < passLength; i++) {
            int pos = ran.nextInt(charString.length());
            
            s.append(charString.charAt(pos));
        }
        return s.toString().toCharArray();
    }

}
