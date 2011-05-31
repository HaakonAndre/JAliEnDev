package alien.services;

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
import alien.tsealedEnvelope.EncryptedAuthzToken;

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
	private static final String SEPrivLocation;
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
		SEPrivLocation = seKeysLocation+"rpriv.pem";
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
		
		BufferedReader sePriv = null;
		BufferedReader sePub  = null;
		
		try{
			sePriv = new BufferedReader(new FileReader(SEPrivLocation)); 
			sePub  = new BufferedReader(new FileReader(SEPubLocation));
			
//			sePrivKey = (RSAPrivateKey) ((KeyPair) new PEMReader(sePriv).readObject()).getPrivate();
			sePubKey = (RSAPublicKey) ((X509Certificate) new PEMReader(sePub).readObject()).getPublicKey();
		}
		catch (IOException ioe){
			logger.log(Level.WARNING, "SE keys could not be loaded from "+seKeysLocation);
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

		// System.out.println("About to be signed: " +
		// envelope.getUnsignedEnvelope());

		final long issued = System.currentTimeMillis() / 1000L;
		final long expires = issued + 86400;

		final String toBeSigned = envelope.getUnsignedEnvelope()
				+ "-issuer-issued-expires&issuer=JAuthenX@"+getLocalHostName()+"&issued=" + issued + "&expires=" + expires;

		final Signature signer = Signature.getInstance("SHA384withRSA");
		
		signer.initSign(AuthenPrivKey);
		
		signer.update(toBeSigned.getBytes());

		final byte[] rawsignature = signer.sign();
		
		envelope.setSignedEnvelope(toBeSigned + "&signature=" + alien.services.Base64.encode(rawsignature));

		System.out.println("We signed: " + envelope.getSignedEnvelope());

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

		HashMap<String, String> env = new HashMap<String, String>();

		String signedEnvelope = "";

		StringTokenizer st = new StringTokenizer(envelope.getSignedEnvelope(), "&");

		while (st.hasMoreTokens()) {
			String tok = st.nextToken();

			int idx = tok.indexOf('=');

			if (idx >= 0) {
				String key = tok.substring(0, idx);
				String value = tok.substring(idx + 1);
				env.put(key, value);
			}
		}
		StringTokenizer hash = new StringTokenizer(env.get("hashord"), "-");

		while (hash.hasMoreTokens()) {
			String key = hash.nextToken();
			if(env.get(key)!=null)
			signedEnvelope += key + "=" + env.get(key) +"&";
		}
		signedEnvelope = signedEnvelope.substring(0, signedEnvelope.length()-1);
		
		System.out.println("verifying sign of: " + signedEnvelope);
		System.out.println("verifying signature: " + env.get("signature"));
		System.out.println();
		final Signature signer = Signature.getInstance("SHA384withRSA");

		if (selfSigned) {
			signer.initVerify(SEPubKey);
		}
		else {
			signer.initVerify(AuthenPubKey);
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
	
//	public static String decrypt(final String s) throws GeneralSecurityException{
//		EncryptedAuthzToken deAuthz = new EncryptedAuthzToken( SEPrivKey, AuthenPubKey ,true);
//
//		return deAuthz.decrypt(s);
//	}
	
	//
	//
	//
	//
	//
	//
	// /**
	// * @param ca
	// * @param sitename
	// * @param qos
	// * @param qosCount
	// * @param staticSEs
	// * @param exxSes
	// */
	// public static void loadXrootDEnvelopesForCatalogueAccess(
	// CatalogueAccess ca, String sitename, String qos, int qosCount,
	// Set<SE> staticSEs, Set<SE> exxSes) {
	//
	// if (ca.getAccess() == CatalogueAccess.READ
	// || ca.getAccess() == CatalogueAccess.DELETE) {
	//
	// ca.loadPFNS();
	//
	// final Set<PFN> whereis = ca.getPFNS();
	//
	// // keep all replicas, even if the SE is reported as broken (though
	// // it will go to the end of list)
	// final List<PFN> sorted = SEUtils.sortBySite(whereis, sitename,
	// false);
	//
	// // remove the replicas from these SEs, if specified
	// if (exxSes != null && exxSes.size() > 0) {
	// final Iterator<PFN> it = sorted.iterator();
	//
	// while (it.hasNext()) {
	// final PFN pfn = it.next();
	//
	// final SE se = SEUtils.getSE(pfn.seNumber);
	//
	// if (exxSes.contains(se))
	// it.remove();
	// }
	// }
	//
	// // getEnvelopesforPFNList(ca, sorted);
	// if (sorted.size() > 0)
	// ca.addEnvelope(new XrootDEnvelope(ca, sorted.get(0)));
	// } else if (ca.getAccess() == CatalogueAccess.WRITE) {
	//
	// final List<SE> ses = new LinkedList<SE>();
	//
	// if (staticSEs != null)
	// ses.addAll(staticSEs);
	//
	// filterSEs(ses, qos, exxSes);
	//
	// if (ses.size() >= qosCount) {
	// final List<SE> dynamics = SEUtils.getClosestSEs(sitename);
	//
	// filterSEs(dynamics, qos, exxSes);
	//
	// final Iterator<SE> it = dynamics.iterator();
	//
	// while (ses.size() < qosCount && it.hasNext())
	// ses.add(it.next());
	// }
	//
	// getEnvelopesforSEList(ca, ses);
	// }
	// }
	//
	// private static final void filterSEs(final List<SE> ses, final String qos,
	// final Set<SE> exxSes) {
	// final Iterator<SE> it = ses.iterator();
	//
	// while (it.hasNext()) {
	// final SE se = it.next();
	//
	// if (!se.qos.contains(qos) || exxSes.contains(se))
	// it.remove();
	// }
	// }
	//
	// private static void getEnvelopesforSEList(final CatalogueAccess ca,
	// final Collection<SE> ses) {
	// for (final SE se : ses) {
	// ca.addEnvelope(new XrootDEnvelope(ca, se, se.seioDaemons
	// + se.seStoragePath + ca.getGUID().toString()));
	// }
	// }

}
