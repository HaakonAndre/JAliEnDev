package alien.io.xrootd.envelopes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;

import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;
import alien.config.JAliEnIAm;

/**
 * @author ron
 * @since Nov 14, 2010
 */
public class XrootDEnvelopeSigner {
	
	/**
	 * logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(XrootDEnvelopeSigner.class.getCanonicalName());

	private static final String JAuthZPrivLocation;
	private static final String JAuthZPubLocation;
	private static final String SEPrivLocation;
	private static final String SEPubLocation;

	private static final RSAPrivateKey JAuthZPrivKey;
	private static final RSAPublicKey JAuthZPubKey;
	private static final RSAPrivateKey SEPrivKey;
	private static final RSAPublicKey SEPubKey;
	
	/**
	 * load the RSA keys for envelope signature, keys are supposed to be in pem,
	 * and can be created with: openssl req -x509 -nodes -days 365 -newkey
	 * rsa:4096 -keyout lpriv.pem -out lpub.pem
	 */
	static{
		Security.addProvider(new BouncyCastleProvider());

		JAuthZPrivLocation = ConfigUtils.getConfig().gets("jAuthZ.priv.key.location", System.getProperty("user.home")+System.getProperty("file.separator")+".alien"+System.getProperty("file.separator")+"authen"+System.getProperty("file.separator")+"lpriv.pem");
		JAuthZPubLocation = ConfigUtils.getConfig().gets("jAuthZ.pub.key.location", System.getProperty("user.home")+System.getProperty("file.separator")+".alien"+System.getProperty("file.separator")+"authen"+System.getProperty("file.separator")+"lpub.pem");
		SEPrivLocation = ConfigUtils.getConfig().gets("SE.priv.key.location", System.getProperty("user.home")+System.getProperty("file.separator")+".alien"+System.getProperty("file.separator")+"authen"+System.getProperty("file.separator")+"rpriv.pem");
		SEPubLocation = ConfigUtils.getConfig().gets("SE.pub.key.location", System.getProperty("user.home")+System.getProperty("file.separator")+".alien"+System.getProperty("file.separator")+"authen"+System.getProperty("file.separator")+"rpub.pem");

		//System.out.println("Using private JAuthZ Key: " + JAuthZPrivLocation + "/" + JAuthZPubLocation);
		//System.out.println("Using private SE Key: " + SEPrivLocation + "/" + SEPubLocation);
		
		
		RSAPrivateKey jAuthZPrivKey = null;
		RSAPublicKey  jAuthZPubKey = null;
		RSAPrivateKey sePrivKey = null;
		RSAPublicKey  sePubKey = null;
		
		BufferedReader jAuthZPriv = null;
		BufferedReader jAuthZPub  = null;
		
		try{
			jAuthZPriv = new BufferedReader(new FileReader(JAuthZPrivLocation));
			jAuthZPub  = new BufferedReader(new FileReader(JAuthZPubLocation));
			
			jAuthZPrivKey = (RSAPrivateKey) ((KeyPair) new PEMReader(jAuthZPriv).readObject()).getPrivate();
			jAuthZPubKey = (RSAPublicKey) ((X509Certificate) new PEMReader(jAuthZPub).readObject()).getPublicKey();
		}
		catch (IOException ioe){
			logger.log(Level.WARNING, "Authen keys could not be loaded from "+JAuthZPrivLocation +"/" + JAuthZPubLocation);
		}
		finally{
			if (jAuthZPriv!=null)
				try {
					jAuthZPriv.close();
				}
				catch (IOException e) {
					// ignore
				}
			
			if (jAuthZPub!=null)
				try {
					jAuthZPub.close();
				}
				catch (IOException e) {
					// ignore
				}
		}
		
		BufferedReader sePriv = null;
		BufferedReader sePub  = null;
		
		try{
			sePriv = new BufferedReader(new FileReader(SEPrivLocation)); 
			sePub  = new BufferedReader(new FileReader(SEPubLocation));
			
			sePrivKey = (RSAPrivateKey) ((KeyPair) new PEMReader(sePriv).readObject()).getPrivate();
			sePubKey = (RSAPublicKey) ((X509Certificate) new PEMReader(sePub).readObject()).getPublicKey();
		}
		catch (IOException ioe){
			logger.log(Level.WARNING, "SE keys could not be loaded from "+SEPrivLocation + "/"+ SEPubLocation);
		}
		finally{
			if (sePriv!=null){
				try {
					sePriv.close();
				}
				catch (IOException e) {
					// ignore
				}
			}
			if (sePub!=null){
				try {
					sePub.close();
				}
				catch (IOException e) {
					// ignore
				}
			}
		}
		
		JAuthZPrivKey = jAuthZPrivKey;
		JAuthZPubKey = jAuthZPubKey;
		SEPrivKey = sePrivKey;
		SEPubKey = sePubKey;
	}
	
	
	private static String localHostName = null;
	
	private static final String getLocalHostName(){
		if (localHostName==null){
			try{
				localHostName = java.net.InetAddress.getLocalHost().getHostName();
			}
			catch (Exception e){
				// ignore
			}
		}
		
		return localHostName;
	}
	
	/**
	 * @param envelope
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 */
	public static void signEnvelope(final XrootDEnvelope envelope)
			throws NoSuchAlgorithmException, InvalidKeyException,
			SignatureException {
		
		final long issued = System.currentTimeMillis() / 1000L;
		final long expires = issued + 60*60*24;

		final String toBeSigned = envelope.getUnsignedEnvelope()
				+ "-issuer-issued-expires&issuer="+JAliEnIAm.whatsMyName()+"_"+getLocalHostName()+
				"&issued=" + issued + "&expires=" + expires;

		final Signature signer = Signature.getInstance("SHA384withRSA");
		
		signer.initSign(JAuthZPrivKey);
		
		signer.update(toBeSigned.getBytes());

		envelope.setSignedEnvelope(toBeSigned + "&signature=" + Base64.encode(signer.sign()));

	}


	/**
	 * @param envelope
	 * @param selfSigned
	 * @return <code>true</code> if the signature verifies
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 */
	public static boolean verifyEnvelope(final XrootDEnvelope envelope, final boolean selfSigned)
			throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {

		return verifyEnvelope(envelope.getSignedEnvelope(), selfSigned);
	}
	
	/**
	 * @param envelope
	 * @param selfSigned
	 * @return <code>true</code> if the signature verifies
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 */
	public static boolean verifyEnvelope(final String envelope, final boolean selfSigned)
			throws NoSuchAlgorithmException, InvalidKeyException, SignatureException {

		HashMap<String, String> env = new HashMap<String, String>();

		StringBuilder signedEnvelope = new StringBuilder();
		
		String sEnvelope = envelope;
		
		if(sEnvelope.contains("\\&"))
			sEnvelope = sEnvelope.replace("\\&", "&");

		StringTokenizer st = new StringTokenizer(sEnvelope, "&");

		while (st.hasMoreTokens()) {
			String tok = st.nextToken();

			int idx = tok.indexOf('=');

			if (idx >= 0) {
				String key = tok.substring(0, idx);
				String value = tok.substring(idx + 1);
				env.put(key, value);			}
		}
		
		StringTokenizer hash = new StringTokenizer(env.get("hashord"), "-");

		while (hash.hasMoreTokens()) {
			String key = hash.nextToken();
			
			if (signedEnvelope.length()>0)
				signedEnvelope.append('&');
			
			signedEnvelope.append(key).append('=').append(env.get(key));
		}
		
		// TODO: this needs to go in already by the SE. Drop it here, when the SE places it itself.
		//System.out.println("envelope is before hashord padding:" + signedEnvelope);
		if(!selfSigned){
			if (signedEnvelope.length()>0)
				signedEnvelope.append('&');
			
			signedEnvelope.append("hashord=").append(env.get("hashord"));
		}
		
		//System.out.println("plain envelope is : " + signedEnvelope);
		//System.out.println("sign for envelope is : " + env.get("signature"));


		final Signature signer = Signature.getInstance("SHA384withRSA");

		if (selfSigned) {
			signer.initVerify(JAuthZPubKey);
		}
		else {
			signer.initVerify(SEPubKey);
		}
		
		signer.update(signedEnvelope.toString().getBytes());

		return signer.verify(Base64.decode(env.get("signature")));
	}

	/**
	 * @param envelope
	 * @throws GeneralSecurityException
	 */
	public static void encryptEnvelope(final XrootDEnvelope envelope) throws GeneralSecurityException {
		final EncryptedAuthzToken authz = new EncryptedAuthzToken(JAuthZPrivKey, SEPubKey, false);
		
		final String plainEnvelope = envelope.getUnEncryptedEnvelope();
		
		if (logger.isLoggable(Level.FINEST))
			logger.log(Level.FINEST, "Encrypting this envelope:\n"+plainEnvelope);
		
		envelope.setEncryptedEnvelope(authz.encrypt(plainEnvelope));
	}
		
	/**
	 * @param envelope
	 * @return a loaded XrootDEnvelope with the verified values
	 * @throws GeneralSecurityException
	 */
	public static XrootDEnvelope decryptEnvelope(final String envelope) throws GeneralSecurityException {
		final EncryptedAuthzToken authz = new EncryptedAuthzToken(SEPrivKey, JAuthZPubKey, true);
		
		return new XrootDEnvelope(authz.decrypt(envelope));
	}
	
	/**
	 * @param envelope
	 * @return the decrypted envelope, for debugging
	 * @throws GeneralSecurityException 
	 */
	public static String decrypt(final String envelope) throws GeneralSecurityException{
		final EncryptedAuthzToken authz = new EncryptedAuthzToken(SEPrivKey, JAuthZPubKey, true);
		
		return authz.decrypt(envelope);
	}

	/**
	 * @param args
	 * @throws GeneralSecurityException
	 * @throws IOException
	 */
	public static void main(String[] args) throws GeneralSecurityException, IOException {
		final StringBuilder sb = new StringBuilder();
		
		String sLine;
		
		while ( (sLine=System.console().readLine())!=null ){
			sb.append(sLine).append("\n");
		}
		
		System.out.println(decrypt(sb.toString()));
	}
	
}
