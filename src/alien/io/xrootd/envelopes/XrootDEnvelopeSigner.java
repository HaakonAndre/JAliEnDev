package alien.io.xrootd.envelopes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.ExtProperties;
import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;

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

	private static final String AuthenPrivLocation;
	private static final String AuthenPubLocation;
//	private static final String SEPrivLocation;
	private static final String SEPubLocation;

	private static final RSAPrivateKey AuthenPrivKey;
	private static final RSAPublicKey AuthenPubKey;
//	private static final RSAPrivateKey SEPrivKey;
	private static final RSAPublicKey SEPubKey;
	
	/**
	 * load the RSA keys for envelope signature, keys are supposed to be in pem,
	 * and can be created with: openssl req -x509 -nodes -days 365 -newkey
	 * rsa:4096 -keyout lpriv.pem -out lpub.pem
	 */
	static{
		Security.addProvider(new BouncyCastleProvider());

		ExtProperties config = ConfigUtils.getConfig();
		
		final String authenKeysLocation = config.gets("Authen.keys.location", System.getProperty("user.home")+System.getProperty("file.separator")+".alien"+System.getProperty("file.separator")+"authen"+System.getProperty("file.separator"));
		
		final String seKeysLocation = config.gets("SE.keys.location", authenKeysLocation);
		
		AuthenPrivLocation = authenKeysLocation+"lpriv.pem";
		AuthenPubLocation = authenKeysLocation+"lpub.pem";
//		SEPrivLocation = seKeysLocation+"rpriv.pem";
		SEPubLocation = seKeysLocation+"rpub.pem";

		RSAPrivateKey authenPrivKey = null;
		RSAPublicKey  authenPubKey = null;
//		RSAPrivateKey sePrivKey = null;
		RSAPublicKey  sePubKey = null;
		
		BufferedReader authenPriv = null;
		BufferedReader authenPub  = null;
		
		try{
			authenPriv = new BufferedReader(new FileReader(AuthenPrivLocation));
			authenPub  = new BufferedReader(new FileReader(AuthenPubLocation));
			
			authenPrivKey = (RSAPrivateKey) ((KeyPair) new PEMReader(authenPriv).readObject()).getPrivate();
			authenPubKey = (RSAPublicKey) ((X509Certificate) new PEMReader(authenPub).readObject()).getPublicKey();
		}
		catch (IOException ioe){
			logger.log(Level.WARNING, "Authen keys could not be loaded from "+authenKeysLocation);
		}
		finally{
			if (authenPriv!=null)
				try {
					authenPriv.close();
				}
				catch (IOException e) {
					// ignore
				}
			
			if (authenPub!=null)
				try {
					authenPub.close();
				}
				catch (IOException e) {
					// ignore
				}
		}
		
//		BufferedReader sePriv = null;
		BufferedReader sePub  = null;
		
		try{
//			sePriv = new BufferedReader(new FileReader(SEPrivLocation)); 
			sePub  = new BufferedReader(new FileReader(SEPubLocation));
			
//			sePrivKey = (RSAPrivateKey) ((KeyPair) new PEMReader(sePriv).readObject()).getPrivate();
			sePubKey = (RSAPublicKey) ((X509Certificate) new PEMReader(sePub).readObject()).getPublicKey();
		}
		catch (IOException ioe){
			logger.log(Level.WARNING, "SE keys could not be loaded from "+seKeysLocation);
		}
		finally{
//			if (sePriv!=null){
//				try {
//					sePriv.close();
//				}
//				catch (IOException e) {
//					// ignore
//				}
//			}
			if (sePub!=null){
				try {
					sePub.close();
				}
				catch (IOException e) {
					// ignore
				}
			}
		}
		
		AuthenPrivKey = authenPrivKey;
		AuthenPubKey = authenPubKey;
//		SEPrivKey = sePrivKey;
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
		final long expires = issued + 86400;

		final String toBeSigned = envelope.getUnsignedEnvelope()
				+ "-issuer-issued-expires&issuer="+JAliEnIAm.whatsMyName()+"@"+getLocalHostName()+
				"&issued=" + issued + "&expires=" + expires;

		final Signature signer = Signature.getInstance("SHA384withRSA");
		
		signer.initSign(AuthenPrivKey);
		
		signer.update(toBeSigned.getBytes());

		envelope.setSignedEnvelope((toBeSigned + "&signature=" + Base64.encode(signer.sign())).replace("&", "\\&"));

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

		String signedEnvelope = "";

		StringTokenizer st = new StringTokenizer(envelope, "\\&");

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
			signedEnvelope += key + "=" + env.get(key) +"&";
		}
		signedEnvelope = signedEnvelope.substring(0, signedEnvelope.lastIndexOf("&"));
		
		// TODO: this needs to go in already by the SE. Drop it here, when the SE places it itself.
		System.out.println("envelope is before hashord padding:" + signedEnvelope);
		if(!selfSigned)
			signedEnvelope += "&" + "hashord=" + env.get("hashord");
		
		System.out.println("plain envelope is : " + signedEnvelope);
		System.out.println("sign for envelope is : " + env.get("signature"));


		final Signature signer = Signature.getInstance("SHA384withRSA");

		if (selfSigned) {
			signer.initVerify(AuthenPubKey);
		}
		else {
			signer.initVerify(SEPubKey);
		}
		signer.update(signedEnvelope.getBytes());

		return signer.verify(Base64.decode(env.get("signature")));
	}

	/**
	 * @param envelope
	 * @throws GeneralSecurityException
	 */
	public static void encryptEnvelope(final XrootDEnvelope envelope) throws GeneralSecurityException {
		final EncryptedAuthzToken authz = new EncryptedAuthzToken(AuthenPrivKey, SEPubKey, false);

		final String plainEnvelope = envelope.getUnEncryptedEnvelope(); 
		
		//System.err.println("Encrypting:\n"+plainEnvelope);
		
		envelope.setEncryptedEnvelope(authz.encrypt(plainEnvelope));
	}

	/**
	 * @param args
	 * @throws GeneralSecurityException
	 * @throws IOException
	 */
	public static void main(String[] args) throws GeneralSecurityException, IOException {
	
		int ok = 0;
		int fail = 0;
		
		while (true){
			String ticket = "<authz>\n"+
			"  <file>\n"+
			"    <access>delete</access>\n"+
			"    <turl>root://pcaliense01.cern.ch:1094//00/23886/CDE428D4-572B-11E0-88BE-001F29EB8B98</turl>\n"+
			"    <lfn>/NOLFN</lfn>\n"+
			"    <size>12345</size>\n"+
			"    <guid>CDE428D4-572B-11E0-88BE-001F29EB8B98</guid>\n"+
			"    <md5>130254d9540d6903fa6f0ab41a132361</md5>\n"+
			"    <pfn>/00/23886/CDE428D4-572B-11E0-88BE-001F29EB8B98</pfn>\n"+
			"    <se>ALICE::CERN::SETEST</se>\n"+
			"  </file>\n"+
			"</authz>";
			
			EncryptedAuthzToken enAuthz = new EncryptedAuthzToken(AuthenPrivKey, SEPubKey,false);

			String enticket = enAuthz.encrypt(ticket);
			
			FileWriter fw = new FileWriter("/tmp/ticket");
			
			fw.write(enticket);
			fw.close();
			
			List<String> command = new ArrayList<String>();
			
			command.add("/home/costing/temp/x/test.sh");
			command.add("/tmp/ticket");
			
			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);

			pBuilder.returnOutputOnExit(true);

			pBuilder.timeout(1, TimeUnit.MINUTES);

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			try {
				exitStatus = pBuilder.start().waitFor();
			} catch (final InterruptedException ie) {
				throw new IOException(
						"Interrupted while waiting for the following command to finish : "
								+ command.toString());
			}
			
			String out = exitStatus.getStdOut();
			
			if (out.indexOf("<authz>")>=0){
				ok++;
			}
			else{
				fail++;
				System.err.println(out);
				
				break;
			}
			System.err.println("OK : "+ok+", fail : "+fail);
		}
	}
	
}
