package alien.services;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import alien.catalogue.PFN;
import alien.catalogue.access.AccessTicket;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.se.SE;
import alien.se.SEUtils;
import alien.tsealedEnvelope.EncryptedAuthzToken;

/**
 * @author ron
 * @since Nov 14, 2010
 */
public class XrootDEnvelopeSigner {


	private RSAPrivateKey AuthenPrivKey;
	private RSAPublicKey AuthenPubKey;
	private RSAPrivateKey SEPrivKey;
	private RSAPublicKey SEPubKey;
	
	EncryptedAuthzToken authz = null;
	
	public XrootDEnvelopeSigner() {
		try {
			loadKeys();
		} catch (GeneralSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	

	
	public void signEnvelopesForAccess(AccessTicket ticket){
		
		if(ticket != null){
			final Iterator<PFN> it = ticket.getPFNS().iterator();

			while (it.hasNext()) {
				final PFN pfn = it.next();

				if(ticket.type != AccessType.DENIED){
					pfn.envelope = new XrootDEnvelope(ticket, pfn, SEUtils.getSE(pfn.seNumber));
					try {
						signEnvelope(pfn.envelope);
					} catch (InvalidKeyException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (NoSuchAlgorithmException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (SignatureException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					if(pfn.envelope.createEcryptedEnvelope)
						try {
							encryptEnvelope(pfn.envelope);
						} catch (GeneralSecurityException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
					
			}
			
		}
		
	}
	
	

	private void loadKeys() throws GeneralSecurityException, IOException {


		Security.addProvider(new BouncyCastleProvider());

		// TODO : load file locations from configuration
		File AuthenPrivfile = new File("/home/ron/authen_keys/AuthenPriv.der");
		byte[] AuthenPriv = new byte[(int) AuthenPrivfile.length()];
		FileInputStream fis = new FileInputStream(AuthenPrivfile);
		fis.read(AuthenPriv);
		fis.close();

		PKCS8EncodedKeySpec AuthenPrivSpec = new PKCS8EncodedKeySpec(AuthenPriv);
		AuthenPrivKey = (RSAPrivateKey) KeyFactory.getInstance("RSA")
				.generatePrivate(AuthenPrivSpec);

		File SEPrivfile = new File("/home/ron/authen_keys/SEPriv.der");
		byte[] SEPriv = new byte[(int) SEPrivfile.length()];
		fis = new FileInputStream(SEPrivfile);
		fis.read(SEPriv);
		fis.close();

		PKCS8EncodedKeySpec SEPrivSpec = new PKCS8EncodedKeySpec(SEPriv);
		SEPrivKey = (RSAPrivateKey) KeyFactory.getInstance("RSA")
				.generatePrivate(SEPrivSpec);

		CertificateFactory certFact = CertificateFactory.getInstance("X.509");

		File AuthenPubfile = new File("/home/ron/authen_keys/AuthenPub.crt");
		X509Certificate AuthenPub = (X509Certificate) certFact
				.generateCertificate(new BufferedInputStream(
						new FileInputStream(AuthenPubfile)));
		AuthenPubKey = (RSAPublicKey) AuthenPub.getPublicKey();

		File SEPubfile = new File("/home/ron/authen_keys/SEPub.crt");

		X509Certificate SEPub = (X509Certificate) certFact
				.generateCertificate(new BufferedInputStream(
						new FileInputStream(SEPubfile)));
		SEPubKey = (RSAPublicKey) SEPub.getPublicKey();

	}
	

	private void signEnvelope(XrootDEnvelope envelope) throws NoSuchAlgorithmException,
			InvalidKeyException, SignatureException {
		
		System.out.println("About to be signed: " + envelope.getUnsignedEnvelope());
		
		long issued = System.currentTimeMillis() / 1000L;
		long expires = issued + 86400;

		String toBeSigned = envelope.getUnsignedEnvelope()
				+ "&issuer=JAuthenX&issued=" + issued + "&expires="
				+ expires + "&hashord=" + envelope.hashord + "-issuer-issued-expires-hashord";
				
		Signature signer = Signature.getInstance("SHA384withRSA");
		signer.initSign(AuthenPrivKey);
		signer.update(toBeSigned.getBytes());
		

		byte[] rawsignature = new byte[1024];
		rawsignature = signer.sign();

		envelope.setSignedEnvelope(toBeSigned + "&signature="
				+ String.valueOf(Base64.encode(rawsignature)));
		
		
		System.out.println("We signed: " + envelope.getSignedEnvelope());

	}
	
	
	private void encryptEnvelope(XrootDEnvelope envelope) throws GeneralSecurityException{
		
		System.out.println("About to be encrypted: " + envelope.getUnEncryptedEnvelope());
		
		if(authz == null)  authz = new EncryptedAuthzToken(AuthenPrivKey, SEPubKey);
		
		envelope.setEncryptedEnvelope(authz.encrypt(envelope.getUnEncryptedEnvelope()));
		System.out.println("We encrypted: " + envelope.getEncryptedEnvelope());
	}
	
//	
//	
//	
//	
//	
//	
//	/**
//	 * @param ca
//	 * @param sitename
//	 * @param qos
//	 * @param qosCount
//	 * @param staticSEs
//	 * @param exxSes
//	 */
//	public static void loadXrootDEnvelopesForCatalogueAccess(
//			CatalogueAccess ca, String sitename, String qos, int qosCount,
//			Set<SE> staticSEs, Set<SE> exxSes) {
//
//		if (ca.getAccess() == CatalogueAccess.READ
//				|| ca.getAccess() == CatalogueAccess.DELETE) {
//
//			ca.loadPFNS();
//
//			final Set<PFN> whereis = ca.getPFNS();
//
//			// keep all replicas, even if the SE is reported as broken (though
//			// it will go to the end of list)
//			final List<PFN> sorted = SEUtils.sortBySite(whereis, sitename,
//					false);
//
//			// remove the replicas from these SEs, if specified
//			if (exxSes != null && exxSes.size() > 0) {
//				final Iterator<PFN> it = sorted.iterator();
//
//				while (it.hasNext()) {
//					final PFN pfn = it.next();
//
//					final SE se = SEUtils.getSE(pfn.seNumber);
//
//					if (exxSes.contains(se))
//						it.remove();
//				}
//			}
//
//			// getEnvelopesforPFNList(ca, sorted);
//			if (sorted.size() > 0)
//				ca.addEnvelope(new XrootDEnvelope(ca, sorted.get(0)));
//		} else if (ca.getAccess() == CatalogueAccess.WRITE) {
//
//			final List<SE> ses = new LinkedList<SE>();
//
//			if (staticSEs != null)
//				ses.addAll(staticSEs);
//
//			filterSEs(ses, qos, exxSes);
//
//			if (ses.size() >= qosCount) {
//				final List<SE> dynamics = SEUtils.getClosestSEs(sitename);
//
//				filterSEs(dynamics, qos, exxSes);
//
//				final Iterator<SE> it = dynamics.iterator();
//
//				while (ses.size() < qosCount && it.hasNext())
//					ses.add(it.next());
//			}
//
//			getEnvelopesforSEList(ca, ses);
//		}
//	}
//
//	private static final void filterSEs(final List<SE> ses, final String qos,
//			final Set<SE> exxSes) {
//		final Iterator<SE> it = ses.iterator();
//
//		while (it.hasNext()) {
//			final SE se = it.next();
//
//			if (!se.qos.contains(qos) || exxSes.contains(se))
//				it.remove();
//		}
//	}
//
//	private static void getEnvelopesforSEList(final CatalogueAccess ca,
//			final Collection<SE> ses) {
//		for (final SE se : ses) {
//			ca.addEnvelope(new XrootDEnvelope(ca, se, se.seioDaemons
//					+ se.seStoragePath + ca.getGUID().toString()));
//		}
//	}

}
