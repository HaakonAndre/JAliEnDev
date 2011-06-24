package alien.user;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.cert.Certificate;

import javax.net.ssl.KeyManagerFactory;

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
	
	private static String challenge = null;

	
	static{
		
				privKey = null; // (RSAPrivateKey) JAKeyStore.ks.getKey("User.cert", JAKeyStore.pass);
			
			
			Certificate[] usercert = null; //JAKeyStore.ks.getCertificateChain("User.cert");
			pubKey = null; //(RSAPublicKey) usercert[0].getPublicKey();

	}
	

	/**
	 * @param pFinder
	 * @throws IOException
	 */
	public static  void loadPrivKey(PasswordFinder pFinder) throws IOException {

		if(privKey==null){
			BufferedReader priv = new BufferedReader(new FileReader(ConfigUtils
					.getConfig().gets("user.cert.priv.location").trim()));

			if(pFinder.getPassword().length==0)
				privKey = (RSAPrivateKey) ((KeyPair) new PEMReader(priv)
					.readObject()).getPrivate();
			else
				privKey = (RSAPrivateKey) ((KeyPair) new PEMReader(priv, pFinder)
				.readObject()).getPrivate();
		}
	}

	/**
	 * @param pubCert
	 */
	public static  void loadPubCert(String pubCert) {

		try {
			StringReader pub = new StringReader(pubCert);
			 cert = ((X509Certificate) new PEMReader(pub)
			.readObject());
			
			pubKey = (RSAPublicKey) cert.getPublicKey();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
		}
	}

	/**
	 * Read public user certificate from config location
	 * @return the public certificate
	 * @throws IOException
	 */
	public static  String readPubCert() throws IOException {

		BufferedReader pub = new BufferedReader(new FileReader(ConfigUtils
				.getConfig().gets("user.cert.pub.location").trim()));
		String pubCert = "";
		String rLine;
		while ((rLine = pub.readLine()) != null) {
			pubCert += rLine + "\n";
		}
		return pubCert;
	}


	/**
	 * Create a challenge
	 * @return the challenge
	 */
	public String challenge() {

		challenge = Long.toString(System.currentTimeMillis());
		return challenge;
	}

	/**
	 * sign the challenge as a response
	 * @param pf
	 * @param challenge
	 * @return the response
	 * @throws SignatureException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 */
	public String response(String challenge)
			throws SignatureException, NoSuchAlgorithmException,
			InvalidKeyException {
		Signature signature = Signature.getInstance("SHA384withRSA");
		signature.initSign(privKey);
		signature.update(challenge.getBytes());
		byte[] signatureBytes = signature.sign();
		return new String(Hex.encode(signatureBytes));
	}

	/**
	 * verify the response as signature
	 * @param pubCertString
	 * @param signature
	 * @return verified or not
	 * @throws SignatureException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 */
	public boolean verify(String pubCertString, String signature)
			throws SignatureException, NoSuchAlgorithmException,
			InvalidKeyException {

		loadPubCert(pubCertString);
		Signature verifier = Signature.getInstance("SHA384withRSA");
		verifier.initVerify(pubKey);
		verifier.update(challenge.getBytes());
		if (verifier.verify(Hex.decode(signature))) {
			System.out.println("Access granted for user: " + cert.getSubjectDN());
			return true;
		} else {
			System.out.println("Access denied");
			return false;
		}
	}

}
