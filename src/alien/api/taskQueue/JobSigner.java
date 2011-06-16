package alien.api.taskQueue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.ExtProperties;
import lazyj.Format;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;

import alien.config.ConfigUtils;
import alien.io.xrootd.envelopes.Base64;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;

/**
 * @author ron
 * @since June 09, 2011
 */
public class JobSigner {

	/**
	 * logger
	 */
	static transient final Logger logger = ConfigUtils
			.getLogger(XrootDEnvelopeSigner.class.getCanonicalName());

	private static final String UserPrivLocation;
	private static final String UserPubLocation;

	private static final RSAPrivateKey UserPrivKey;
	private static final RSAPublicKey UserPubKey;


	/**
	 * load the RSA keys for envelope signature, keys are supposed to be in pem,
	 * and can be created with: openssl req -x509 -nodes -days 365 -newkey
	 * rsa:4096 -keyout lpriv.pem -out lpub.pem
	 */
	static {
		Security.addProvider(new BouncyCastleProvider());

		ExtProperties config = ConfigUtils.getConfig();

		final String UserKeysLocation = config.gets(
				"User.keys.location",
				System.getProperty("user.home")
						+ System.getProperty("file.separator") + ".alien"
						+ System.getProperty("file.separator") + "globus"
						+ System.getProperty("file.separator"));

		UserPrivLocation = UserKeysLocation + "userkey.pem";
		UserPubLocation = UserKeysLocation + "usercert.pem";
		RSAPrivateKey userPrivKey = null;
		RSAPublicKey userPubKey = null;
		BufferedReader UserPriv = null;
		BufferedReader UserPub = null;

		try {
			UserPriv = new BufferedReader(new FileReader(UserPrivLocation));
			UserPub = new BufferedReader(new FileReader(UserPubLocation));

			userPrivKey = (RSAPrivateKey) ((KeyPair) new PEMReader(UserPriv)
					.readObject()).getPrivate();
//			final PasswordFinder pFinder = new PasswordFinder() {
//				
//				@Override
//				public char[] getPassword() {
//					// TODO Auto-generated method stub
//					return null;
//				}
//			};;;
//			char[] password = pFinder.getPassword();
//			if (password == null) {
//				throw new PasswordException(
//						"Password is null, but a password is required");
//			}
			userPubKey = (RSAPublicKey) ((X509Certificate) new PEMReader(
					UserPub).readObject()).getPublicKey();

		} catch (IOException ioe) {
			logger.log(Level.WARNING, "User keys could not be loaded from "
					+ UserKeysLocation);
			ioe.printStackTrace();

		} finally {
			if (UserPriv != null)
				try {
					UserPriv.close();
				} catch (IOException e) {
					// ignore
				}

			if (UserPub != null)
				try {
					UserPub.close();
				} catch (IOException e) {
					// ignore
				}

		}

		UserPrivKey = userPrivKey;
		UserPubKey = userPubKey;
	}


	/**
	 * @param origjdl 
	 * @param user 
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @return the signature of the jdl
	 */
	public static String signJob(String origjdl, String user)
			throws NoSuchAlgorithmException, InvalidKeyException,
			SignatureException {

		String jdl = "USER=\"" + user + "\";\n";
		// jdl += "DN=\"" + "sschrein" + "\";\n";

		final long issued = System.currentTimeMillis() / 1000L;
		jdl += "ISSUED=" + issued + ";\n";
		jdl += "EXPIRES=" + (issued + 60*60*24*14) + ";\n";

		jdl += origjdl;
		final Signature signer = Signature.getInstance("SHA384withRSA");

		signer.initSign(UserPrivKey);

		signer.update(jdl.getBytes());
		
		jdl = "\n<JDLSignatureSTART>\n" + jdl + "\n<JDLSignatureSTOP>";

		jdl += "\nSIGNTATURE=\"" + Base64.encode(signer.sign()) + "\";\n";
		return jdl;
	}

	/**
	 * @param jdl
	 * @return <code>true</code> if the signature verifies
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 */
	public static boolean verifyEnvelope(final String origjdl)
			throws NoSuchAlgorithmException, InvalidKeyException,
			SignatureException {
		
		System.out.println("we are verifying as JDL...");

		
		String jdl = origjdl.substring(origjdl.indexOf("<JDLSignatureSTART>\n",origjdl.lastIndexOf("\n<JDLSignatureSTOP>")));
		String signature = origjdl.substring(origjdl.lastIndexOf("SIGNATURE="));
		signature = signature.substring(signature.indexOf('"'),signature.lastIndexOf('"'));
		
		System.out.println("JDL: "+ jdl);
		System.out.println("SIG: " + signature);


		//String jdl = "USER=\"" + user + "\";\n";
		

		//final long issued = System.currentTimeMillis() / 1000L;
		//jdl += "ISSUED=" + issued + ";\n";
		//jdl += "EXPIRES=" + (issued + 60*60*24*14) + ";\n";

		
		 final Signature verifyer = Signature.getInstance("SHA384withRSA");
		
		 verifyer.initVerify(UserPubKey);
		
		 verifyer.update(jdl.getBytes());
		
		 return verifyer.verify(Base64.decode(signature));
	}

}
