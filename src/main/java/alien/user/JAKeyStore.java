package alien.user;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import alien.catalogue.CatalogueUtils;
import alien.config.ConfigUtils;
import lazyj.ExtProperties;

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
	private static KeyStore clientCert = null;

	/**
	 *
	 */
	public static KeyStore tokenCert = null;

	/**
	 * Token can be stored as a string in environment
	 */
	private static String tokenCertString = null;
	private static String tokenKeyString = null;

	/**
	 *
	 */
	private static KeyStore hostCert = null;

	/**
	 *
	 */
	public static KeyStore trustStore;

	/**
	 *
	 */
	public static final char[] pass = getRandomString();

	/**
	 *
	 */
	public static TrustManager trusts[];

	static {
		Security.addProvider(new BouncyCastleProvider());
		try {
			trustStore = KeyStore.getInstance("JKS");
			trustStore.load(null, pass);
			loadTrusts(trustStore);
		}
		catch (final KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
			logger.log(Level.SEVERE, "Exception during loading trust stores (static block)", e);
			e.printStackTrace();
		}

	}

	private static void loadTrusts(final KeyStore keystore) {
		final String trustsDirSet = ConfigUtils.getConfig().gets("trusted.certificates.location",
				UserFactory.getUserHome() + System.getProperty("file.separator") + ".j" + System.getProperty("file.separator") + "trusts");

		try {
			final StringTokenizer st = new StringTokenizer(trustsDirSet, ":");

			// total number of certificates loaded from the ":" separated folder list in the above configuration/environment variable
			int iLoaded = 0;

			while (st.hasMoreTokens()) {
				final File trustsDir = new File(st.nextToken().trim());

				if (logger.isLoggable(Level.INFO))
					logger.log(Level.INFO, "Loading trusts from " + trustsDir.getAbsolutePath());

				final File[] dirContents;

				if (trustsDir.exists() && trustsDir.isDirectory() && (dirContents = trustsDir.listFiles()) != null) {
					final CertificateFactory cf = CertificateFactory.getInstance("X.509");

					for (final File trust : dirContents)
						if (trust.getName().endsWith("der") || trust.getName().endsWith(".0"))
							try (FileInputStream fis = new FileInputStream(trust)) {
								final X509Certificate c = (X509Certificate) cf.generateCertificate(fis);
								if (logger.isLoggable(Level.FINE))
									logger.log(Level.FINE, "Trusting now: " + c.getSubjectDN());

								keystore.setEntry(trust.getName().substring(0, trust.getName().lastIndexOf('.')), new KeyStore.TrustedCertificateEntry(c), null);

								iLoaded++;
							}
							catch (final Exception e) {
								e.printStackTrace();
							}

					if (iLoaded == 0)
						logger.log(Level.WARNING, "No CA files found in " + trustsDir.getAbsolutePath());
					else
						logger.log(Level.INFO, "Loaded " + iLoaded + " certificates from " + trustsDir.getAbsolutePath());
				}
			}

			if (iLoaded == 0)
				try (InputStream classpathTrusts = JAKeyStore.class.getClassLoader().getResourceAsStream("trusted_authorities.jks")) {
					keystore.load(classpathTrusts, "castore".toCharArray());
					logger.log(Level.WARNING, "Found " + keystore.size() + " default trusted CAs in classpath");
				}
				catch (final Throwable t) {
					logger.log(Level.SEVERE, "Cannot load the default trust keystore from classpath", t);
				}

			if (keystore.equals(trustStore)) {
				final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
				tmf.init(trustStore);
				trusts = tmf.getTrustManagers();
			}
		}
		catch (final KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
			logger.log(Level.WARNING, "Exception during loading trust stores", e);
		}
	}

	/**
	 * Check file permissions of certificate and key
	 *
	 * @param user_key
	 *            path to key
	 */
	private static boolean checkKeyPermissions(final String user_key) {
		File key = new File(user_key);

		try {
			if (!user_key.equals(key.getCanonicalPath()))
				key = new File(key.getCanonicalPath());

			if (key.exists() && key.canRead()) {
				final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(key.toPath());

				boolean anyChange = false;

				for (final PosixFilePermission toRemove : EnumSet.range(PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_EXECUTE))
					if (permissions.remove(toRemove))
						anyChange = true;

				if (anyChange) {
					Files.setPosixFilePermissions(key.toPath(), permissions);
				}

				return true;
			}
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Error checking or modifying permissions on " + user_key, e);
		}

		return false;
	}

  private static boolean isEncrypted(String path) {
    try (Scanner scanner = new Scanner(new File(path))) {
      while (scanner.hasNext()) {
        final String nextToken = scanner.next();
        if (nextToken.contains("ENCRYPTED")) {
          return true;
        }
      }
    } finally {
      return false;
    }
  }

	private static boolean loadClientKeyStorage() {
    String defaultKeyPath = Paths.get(UserFactory.getUserHome(), ".globus", "userkey.pem").toString();
    final String user_key = selectPath("X509_USER_KEY", "user.cert.priv.location", defaultKeyPath);

    String defaultCertPath = Paths.get(UserFactory.getUserHome(), ".globus", "usercert.pem").toString();
    final String user_cert = selectPath("X509_USER_KEY", "user.cert.pub.location", defaultCertPath);

    if(user_key == null || user_cert == null) {
      return false;
    }

		if (!checkKeyPermissions(user_key)) {
			logger.log(Level.WARNING, "Permissions on usercert.pem or userkey.pem are not OK");
			return false;
		}

    boolean success = false;
    String password = "";

    try {
      logger.log(Level.SEVERE, "Trying to load USER CERT");
      clientCert = KeyStore.getInstance("JKS");
      clientCert.load(null, pass);
      loadTrusts(clientCert);

      addKeyPairToKeyStore(clientCert, "User.cert", user_key, user_cert, password);
      logger.log(Level.SEVERE, "Loaded USER CERT");
      success = true;
    } catch (final Exception e) {
      logger.log(Level.SEVERE, "Error loading UESR CERT", e);
      System.err.println("Error loading USER CERT");
      success = false;
    }

		return success;
	}

	/**
	 * @param certString
	 * @param keyString
	 * @throws Exception
	 */
	public static void createTokenFromString(final String certString, final String keyString) throws Exception {
		tokenCertString = certString;
		tokenKeyString = keyString;

		loadKeyStore();
	}

	/**
	 * @return true if ok
	 * @throws Exception
	 */
public static String selectPath(String var, String key, String fsPath) {
		final ExtProperties config = ConfigUtils.getConfig();
		final String sUserId = UserFactory.getUserID();

    if (var != null && System.getenv(var) != null) {
      return System.getenv(var);
    } else if (key != null && config.gets(key) != null && config.gets(key) != "") {
      return config.gets(key);
    } else if (fsPath != null && !fsPath.equals("")) {
      return fsPath;
    } else {
      return null;
    }
  }

	public static boolean loadTokenKeyStorage() {
		final String sUserId = UserFactory.getUserID();
		final String tmpDir  = System.getProperty("java.io.tmpdir");
    	boolean success = false;

    final String tokenKeyFilename  = "tokenkey_"  + sUserId + ".pem";
    final String defaultTokenKeyPath = Paths.get(tmpDir, tokenKeyFilename).toString();
		final String token_key;

		if (tokenKeyString != null) {
			token_key = tokenKeyString;
    }
    else {
      token_key = selectPath("JALIEN_TOKEN_KEY", "tokenkey.path", defaultTokenKeyPath);
    }

    final String tokenCertFilename = "tokencert_" + sUserId + ".pem";
    final String defaultTokenCertPath = Paths.get(tmpDir, tokenCertFilename).toString();
		final String token_cert;

		if (tokenCertString != null) {
			token_cert = tokenCertString;
    }
    else {
      token_cert = selectPath("JALIEN_TOKEN_CERT", "tokencert.path", defaultTokenCertPath);
    }

    try {
      logger.log(Level.SEVERE, "Trying to load TOKEN CERT");

      tokenCert = KeyStore.getInstance("JKS");
      tokenCert.load(null, pass);
      loadTrusts(tokenCert);

      addKeyPairToKeyStore(tokenCert, "User.cert", token_key, token_cert, null);
      logger.log(Level.SEVERE, "Loaded TOKEN CERT");

      success = true;
    } catch (final Exception e) {
      logger.log(Level.SEVERE, "Error loading TOKEN CERT", e);
      System.err.println("Error loading TOKEN CERT");

      success = false;
    }

		return success;
	}


	/**
	 * @return true if keystore is loaded successfully
	 * @throws Exception
	 */
	private static boolean loadServerKeyStorage() {
  	String defaultKeyPath = Paths.get(UserFactory.getUserHome(), ".globus", "hostkey.pem").toString();
  	final String host_key = selectPath(null, "host.cert.priv.location", defaultKeyPath);

    String defaultCertPath = Paths.get(UserFactory.getUserHome(), ".globus", "hostcert.pem").toString();
    final String host_cert = selectPath(null, "host.cert.pub.location", defaultCertPath);

    boolean success = false;
    try {
      logger.log(Level.SEVERE, "Trying to load HOST CERT");

      hostCert = KeyStore.getInstance("JKS");
      hostCert.load(null, pass);
      loadTrusts(hostCert);

      addKeyPairToKeyStore(hostCert, "User.cert", host_key, host_cert, null);
      logger.log(Level.SEVERE, "Loaded HOST CERT");
      success = true;
    } catch (final Exception e) {
      logger.log(Level.SEVERE, "Error loading HOST CERT", e);
      System.err.println("Error loading HOST CERT");
      success = false;
    }

		return success;
	}

	private static JPasswordFinder getPassword() {

		final Console cons = System.console();
		Reader isr = null;
		if (cons == null)
			isr = new InputStreamReader(System.in);
		else {
			final char[] passwd = cons.readPassword("Grid certificate password: ");
			final String password = String.valueOf(passwd);
			isr = new StringReader(password);
		}

		final BufferedReader in = new BufferedReader(isr);

		try {
			final String line = in.readLine();

			if (line != null && line.length() > 0)
				return new JPasswordFinder(line.toCharArray());
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Could not read passwd from System.in .", e);
		}

		return new JPasswordFinder("".toCharArray());
	}

	@SuppressWarnings("unused")
	private static void createKeyStore(final KeyStore ks, final String keyStoreName) {

		// pass = getRandomString();

		try (FileInputStream f = new FileInputStream(keyStoreName)) {
			try {
				ks.load(null, pass);
			}
			catch (final NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			catch (final CertificateException e) {
				e.printStackTrace();
			}
			catch (final IOException e) {
				e.printStackTrace();
			}
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Exception creating key store", e);
		}
	}

	private static void addKeyPairToKeyStore(final KeyStore ks, final String entryBaseName, final String privKeyLocation, final String pubKeyLocation, final String password) throws Exception {
  	ks.setEntry(entryBaseName, new KeyStore.PrivateKeyEntry(loadPrivX509(privKeyLocation, password.toCharArray()), loadPubX509(pubKeyLocation, true)), new KeyStore.PasswordProtection(pass));
	}

	private static void addKeyPairToKeyStore(final KeyStore ks, final String entryBaseName, final KeyPair pair, final ArrayList<X509Certificate> chain) throws Exception {

		X509Certificate[] certArray = new X509Certificate[chain.size()];
		certArray = chain.toArray(certArray);
		ks.setEntry(entryBaseName, new KeyStore.PrivateKeyEntry(pair.getPrivate(), certArray), new KeyStore.PasswordProtection(pass));
	}

	/**
	 * @param ks
	 * @param filename
	 * @param password
	 * @return <code>true</code> if the keystore was successfully saved to the target path, <code>false</code> if not
	 */
	public static boolean saveKeyStore(final KeyStore ks, final String filename, final char[] password) {
		if (ks == null) {
			logger.log(Level.WARNING, "Null key store to write to " + filename);
			return false;
		}

		final File f = new File(filename);

		try (FileOutputStream fo = new FileOutputStream(f)) {
			final Set<PosixFilePermission> attrs = new HashSet<>();
			attrs.add(PosixFilePermission.OWNER_READ);
			attrs.add(PosixFilePermission.OWNER_WRITE);

			try {
				Files.setPosixFilePermissions(f.toPath(), attrs);
			}
			catch (final IOException io2) {
				logger.log(Level.WARNING, "Could not protect your keystore " + filename + " with POSIX attributes", io2);
			}

			try {
				ks.store(fo, password);
			}
			catch (final KeyStoreException e) {
				e.printStackTrace();
			}
			catch (final NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			catch (final CertificateException e) {
				e.printStackTrace();
			}
			catch (final IOException e) {
				e.printStackTrace();
			}

			return true;
		}
		catch (final IOException e1) {
			logger.log(Level.WARNING, "Exception saving key store", e1);
		}

		return false;
	}

	/**
	 * @param keyFileLocation
	 * @param password
	 * @return priv key
	 * @throws IOException
	 * @throws PEMException
	 * @throws OperatorCreationException
	 * @throws PKCSException
	 */
	public static PrivateKey loadPrivX509(final String keyFileLocation, final char[] password) throws IOException, PEMException, OperatorCreationException, PKCSException {

		if (logger.isLoggable(Level.INFO))
			logger.log(Level.INFO, "Loading private key: " + keyFileLocation);

		Reader source = null;
		try {
			source = new FileReader(keyFileLocation);
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			source = new StringReader(keyFileLocation);
		}

		try (PEMParser reader = new PEMParser(new BufferedReader(source))) {
			Object obj;
			while ((obj = reader.readObject()) != null) {
				if (obj instanceof PEMEncryptedKeyPair) {
					final PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder().build(password);
					final JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");

					final KeyPair kp = converter.getKeyPair(((PEMEncryptedKeyPair) obj).decryptKeyPair(decProv));

					return kp.getPrivate();
				}

				if (obj instanceof PEMKeyPair)
					obj = ((PEMKeyPair) obj).getPrivateKeyInfo();
				// and let if fall through the next case

				if (obj instanceof PKCS8EncryptedPrivateKeyInfo) {
					final InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(password);

					obj = ((PKCS8EncryptedPrivateKeyInfo) obj).decryptPrivateKeyInfo(pkcs8Prov);
				}

				if (obj instanceof PrivateKeyInfo) {
					final JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");

					return converter.getPrivateKey(((PrivateKeyInfo) obj));
				}

				if (obj instanceof PrivateKey)
					return (PrivateKey) obj;

				if (obj instanceof KeyPair)
					return ((KeyPair) obj).getPrivate();

				System.err.println("Unknown object type: " + obj + "\n" + obj.getClass().getCanonicalName());
			}

			return null;
		}
	}

	/**
	 * @param certFileLocation
	 * @param checkValidity
	 * @return Cert chain
	 */
	public static X509Certificate[] loadPubX509(final String certFileLocation, final boolean checkValidity) {

		if (logger.isLoggable(Level.INFO))
			logger.log(Level.INFO, "Loading public key: " + certFileLocation);

		Reader source = null;
		try {
			source = new FileReader(certFileLocation);
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			source = new StringReader(certFileLocation);
		}

		try (PEMParser reader = new PEMParser(new BufferedReader(source))) {
			Object obj;

			final ArrayList<X509Certificate> chain = new ArrayList<>();

			while ((obj = reader.readObject()) != null)
				if (obj instanceof X509Certificate) {
					try {
						((X509Certificate) obj).checkValidity();
					}
					catch (final CertificateException e) {
						logger.log(Level.SEVERE, "Your certificate has expired or is invalid!", e);
						System.err.println("Your certificate has expired or is invalid:\n  " + e.getMessage());
						reader.close();
						return null;
					}
					chain.add((X509Certificate) obj);
				}
				else if (obj instanceof X509CertificateHolder) {
					final X509CertificateHolder ch = (X509CertificateHolder) obj;

					try {
						final X509Certificate c = new JcaX509CertificateConverter().setProvider("BC").getCertificate(ch);

						if (checkValidity)
							c.checkValidity();

						chain.add(c);
					}
					catch (final CertificateException ce) {
						logger.log(Level.SEVERE, "Exception loading certificate", ce);
					}
				}
				else
					System.err.println("Unknown object type: " + obj + "\n" + obj.getClass().getCanonicalName());

			if (chain.size() > 0)
				return chain.toArray(new X509Certificate[0]);

			return null;
		}
		catch (final IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	private static class JPasswordFinder {

		private final char[] password;

		JPasswordFinder(final char[] password) {
			this.password = password;
		}

		public char[] getPassword() {
			return Arrays.copyOf(password, password.length);
		}
	}

	private static final String charString = "!0123456789abcdefghijklmnopqrstuvwxyz@#$%^&*()-+=_{}[]:;|?/>.,<";

	/**
	 * @return randomized char array of passLength length
	 */
	public static char[] getRandomString() {
		final Random ran = new Random(System.nanoTime());
		final StringBuffer s = new StringBuffer();
		for (int i = 0; i < passLength; i++) {
			final int pos = ran.nextInt(charString.length());

			s.append(charString.charAt(pos));
		}
		return s.toString().toCharArray();
	}

	private static boolean keystore_loaded = false;

	/**
	 * @return true if JAliEn managed to load one of keystores
	 */
	public static boolean loadKeyStore() {
		keystore_loaded = false;

		// If JALIEN_TOKEN_CERT env var is set, token is in highest priority
		if (System.getenv("JALIEN_TOKEN_CERT") != null || tokenCertString != null) {
      keystore_loaded = loadTokenKeyStorage();
      return keystore_loaded;
    }

    if(!keystore_loaded) keystore_loaded = loadClientKeyStorage();
    if(!keystore_loaded) keystore_loaded = loadServerKeyStorage();
    if(!keystore_loaded) keystore_loaded = loadTokenKeyStorage();

    return keystore_loaded;
	}

	/**
	 * @return either tokenCert, clientCert or hostCert keystore
	 */
	public static KeyStore getKeyStore() {
		if (!keystore_loaded)
			loadKeyStore();

		if (System.getenv("JALIEN_TOKEN_CERT") != null || tokenCertString != null) {
			if (JAKeyStore.tokenCert != null) {
				return JAKeyStore.tokenCert;
			}
		}

		if (JAKeyStore.clientCert != null) {
			try {
				if (clientCert.getCertificateChain("User.cert") == null) {
					loadKeyStore();
				}
			}
			catch (final KeyStoreException e) {
				logger.log(Level.SEVERE, "Exception during loading client cert");
				e.printStackTrace();
			}
			return JAKeyStore.clientCert;
		}
		else if (JAKeyStore.hostCert != null) {
			try {
				if (hostCert.getCertificateChain("User.cert") == null)
					loadKeyStore();
			}
			catch (final KeyStoreException e) {
				logger.log(Level.SEVERE, "Exception during loading host cert");
				e.printStackTrace();
			}
			return JAKeyStore.hostCert;
		}
		else if (JAKeyStore.tokenCert != null) {
			try {
				if (tokenCert.getCertificateChain("User.cert") == null)
					loadKeyStore();
			}
			catch (final KeyStoreException e) {
				logger.log(Level.SEVERE, "Exception during loading token cert");
				e.printStackTrace();
			}
			return JAKeyStore.tokenCert;
		}

		return null;
	}
}
