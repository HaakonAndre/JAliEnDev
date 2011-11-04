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
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

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
		
		envelope.setEncryptedEnvelope(authz.encrypt(envelope.getUnEncryptedEnvelope()));
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
			
			EncryptedAuthzToken enAuthz = new EncryptedAuthzToken(JAuthZPrivKey, SEPubKey,false);

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
