package alien.user;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

import lazyj.Utils;

import org.bouncycastle.openssl.PEMReader;
import org.bouncycastle.openssl.PasswordFinder;
import org.bouncycastle.util.encoders.Hex;

import alien.config.ConfigUtils;

/**
 * @author ron
 * @since Jun 17, 2011
 */
public class AuthenticationChecker {

	private static RSAPrivateKey privKey = null;
	private static RSAPublicKey pubKey = null;
	private static X509Certificate cert = null;

	private String challenge = null;

	static {
		privKey = null; // (RSAPrivateKey) JAKeyStore.ks.getKey("User.cert", JAKeyStore.pass);

		//Certificate[] usercert = null; // JAKeyStore.ks.getCertificateChain("User.cert");
		pubKey = null; // (RSAPublicKey) usercert[0].getPublicKey();
	}

	/**
	 * @param pFinder
	 * @throws IOException
	 */
	public static void loadPrivKey(PasswordFinder pFinder) throws IOException {

		if (privKey == null) {
			BufferedReader priv = null;

			PEMReader reader = null;

			try {
				priv = new BufferedReader(new FileReader(ConfigUtils.getConfig().gets("user.cert.priv.location").trim()));

				if (pFinder.getPassword().length == 0)
					reader = new PEMReader(priv);
				else
					reader = new PEMReader(priv, pFinder);

				privKey = (RSAPrivateKey) ((KeyPair) reader.readObject()).getPrivate();
			}
			finally {
				try {
					if (reader != null) {
						reader.close();
					}
				}
				catch (IOException ioe) {
					// ignore
				}

				try {
					if (priv != null)
						priv.close();
				}
				catch (IOException ioe) {
					// ignore
				}
			}
		}
	}

	/**
	 * @param pubCert
	 */
	public static void loadPubCert(final String pubCert) {

		PEMReader pemReader = null;
		
		try {
			final StringReader pub = new StringReader(pubCert);
			
			pemReader = new PEMReader(pub);
			
			cert = ((X509Certificate) pemReader.readObject());

			pubKey = (RSAPublicKey) cert.getPublicKey();
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
		finally{
			if (pemReader!=null){
				try{
					pemReader.close();
				}
				catch (final IOException ioe){
					// ignore
				}
			}
		}
	}

	/**
	 * Read public user certificate from config location
	 * 
	 * @return the public certificate
	 * @throws IOException
	 */
	public static String readPubCert() throws IOException {
		String location = ConfigUtils.getConfig().gets("user.cert.pub.location").trim();

		return Utils.readFile(location);
	}

	/**
	 * Create a challenge
	 * 
	 * @return the challenge
	 */
	public String challenge() {
		challenge = Long.toString(System.currentTimeMillis());
		return challenge;
	}

	/**
	 * sign the challenge as a response
	 * 
	 * @param challengeText
	 * @return the response
	 * @throws SignatureException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 */
	public static String response(final String challengeText) throws SignatureException, NoSuchAlgorithmException, InvalidKeyException {
		Signature signature = Signature.getInstance("SHA384withRSA");
		signature.initSign(privKey);
		signature.update(challengeText.getBytes());
		byte[] signatureBytes = signature.sign();
		return new String(Hex.encode(signatureBytes));
	}

	/**
	 * verify the response as signature
	 * 
	 * @param pubCertString
	 * @param signature
	 * @return verified or not
	 * @throws SignatureException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 */
	public boolean verify(String pubCertString, String signature) throws SignatureException, NoSuchAlgorithmException, InvalidKeyException {

		loadPubCert(pubCertString);
		Signature verifier = Signature.getInstance("SHA384withRSA");
		verifier.initVerify(pubKey);
		verifier.update(challenge.getBytes());
		if (verifier.verify(Hex.decode(signature))) {
			System.out.println("Access granted for user: " + cert.getSubjectDN());
			return true;
		}

		System.out.println("Access denied");
		return false;
	}
}
