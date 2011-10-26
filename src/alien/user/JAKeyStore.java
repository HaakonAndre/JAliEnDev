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

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import lazyj.ExtProperties;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;
import org.bouncycastle.openssl.PasswordFinder;

import alien.config.ConfigUtils;

/**
 * 
 * @author ron
 * @since Jun 22, 2011
 */
public class JAKeyStore {

	
	
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

//	/**
//	 * 
//	 */
//	public static KeyStore authenKeys;
//	/**
//	 * 
//	 */
//	public static KeyStore seKeys;

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

		loadTrustedCertificates();

	}

	private static void loadTrustedCertificates() {
		try {
		//	trustStore = KeyStore.getInstance("JKS");

		//	trustStore.load(null, pass);

		//	TrustManagerFactory tmf;

		//	tmf = TrustManagerFactory.getInstance("SunX509");

			File trustsDir = new File(ConfigUtils.getConfig().gets(
					"trusted.certificates.location",
					System.getProperty("user.home")
							+ System.getProperty("file.separator") + ".alien"
							+ System.getProperty("file.separator") + "trusted"));

			if (trustsDir.exists() && trustsDir.isDirectory()) {
				CertificateFactory cf;

				cf = CertificateFactory.getInstance("X.509");

				trustedCertificates = new X509Certificate[trustsDir.listFiles().length+1];
				int ccount = 1;
				
				for (File trust : trustsDir.listFiles()) {

					if (trust.getName().endsWith("der")) {

						try {
							X509Certificate c = (X509Certificate) cf
									.generateCertificate(new FileInputStream(
											trust));
							System.out.println("Trusting now: "
									+ c.getSubjectDN());
							trustedCertificates[ccount] = c;
							ccount++;

						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}

			//tmf.init(trustStore);
			//trusts = tmf.getTrustManagers();

			// for (TrustManager trustManager :
			// trustManagerFactory.getTrustManagers()) {
			// System.out.println(trustManager);
			//
			// if (trustManager instanceof X509TrustManager) {
			// X509TrustManager x509TrustManager =
			// (X509TrustManager)trustManager;
			// System.out.println("\tAccepted issuers count : " +
			// x509TrustManager.getAcceptedIssuers().length);
			// }
			// }
		} catch (Exception e) {
			e.printStackTrace();
		}
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
							System.out.println("Trusting now: "
									+ c.getSubjectDN());
									
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

			// for (TrustManager trustManager :
			// trustManagerFactory.getTrustManagers()) {
			// System.out.println(trustManager);
			//
			// if (trustManager instanceof X509TrustManager) {
			// X509TrustManager x509TrustManager =
			// (X509TrustManager)trustManager;
			// System.out.println("\tAccepted issuers count : " +
			// x509TrustManager.getAcceptedIssuers().length);
			// }
			// }
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
		 jpf = new JPasswordFinder(new char[]{});
		else
		 jpf =	getPassword("Grid certificate");
		
		
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
//		authenKeys = KeyStore.getInstance("JKS");
//		authenKeys.load(null, pass);
//
//		seKeys = KeyStore.getInstance("JKS");
//		seKeys.load(null, pass);
		
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

//		addKeyPairToKeyStore(
//				authenKeys,
//				"Authen.keys",
//				config.gets(
//						"Authen.keys.location",
//						System.getProperty("user.home")
//								+ System.getProperty("file.separator")
//								+ ".alien"
//								+ System.getProperty("file.separator")
//								+ "authen"
//								+ System.getProperty("file.separator"))
//						+ "lpriv.pem",
//				config.gets(
//						"Authen.keys.location",
//						System.getProperty("user.home")
//								+ System.getProperty("file.separator")
//								+ ".alien"
//								+ System.getProperty("file.separator")
//								+ "authen"
//								+ System.getProperty("file.separator"))
//						+ "lpub.pem", null);
//
//		addKeyPairToKeyStore(
//				seKeys,
//				"SE.keys",
//				config.gets(
//						"Authen.keys.location",
//						System.getProperty("user.home")
//								+ System.getProperty("file.separator")
//								+ ".alien"
//								+ System.getProperty("file.separator")
//								+ "authen"
//								+ System.getProperty("file.separator"))
//						+ "rpriv.pem",
//				config.gets(
//						"Authen.keys.location",
//						System.getProperty("user.home")
//								+ System.getProperty("file.separator")
//								+ ".alien"
//								+ System.getProperty("file.separator")
//								+ "authen"
//								+ System.getProperty("file.separator"))
//						+ "rpub.pem", null);

		loadTrusts();

	}

	private static JPasswordFinder getPassword(String consoleMessage) {

		String pass = "";
		Console cons;
		char[] passwd = new char[] {};
		if ((cons = System.console()) == null)
			System.out
					.println("Could not get console to request key password.");

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

			// ks.getDefaultType();
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
		System.out.println("Loading private key ... " + keyFileLocation);

		BufferedReader priv = new BufferedReader(
				new FileReader(keyFileLocation));

		if (pFinder == null)
			return ((KeyPair) new PEMReader(priv).readObject()).getPrivate();

		return ((KeyPair) new PEMReader(priv, pFinder).readObject())
				.getPrivate();

	}

	/**
	 * @param keyFileLocation
	 * @return Cert chain
	 * @throws IOException
	 */
	public static Certificate[] loadPubX509(String keyFileLocation)
			throws IOException {

		System.out.println("Loading public ... " + keyFileLocation);
		BufferedReader pub = new BufferedReader(new FileReader(keyFileLocation));
		try {
			//trustedCertificates[0] = (X509Certificate) new PEMReader(pub).readObject();
			//return trustedCertificates;
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
