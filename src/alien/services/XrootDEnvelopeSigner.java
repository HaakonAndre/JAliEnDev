package alien.services;

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
import java.util.Iterator;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;

import alien.catalogue.PFN;
import alien.catalogue.access.AccessTicket;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.se.SEUtils;
import alien.tsealedEnvelope.EncryptedAuthzToken;

/**
 * @author ron
 * @since Nov 14, 2010
 */
public class XrootDEnvelopeSigner {

	private String AuthenPrivLocation = "/home/ron/authen_keys/lpriv.pem";
	private String AuthenPubLocation = "/home/ron/authen_keys/lpub.pem";
	private String SEPrivLocation = "/home/ron/authen_keys/rpriv.pem";
	private String SEPubLocation = "/home/ron/authen_keys/rpub.pem";

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

	public void signEnvelopesForAccess(AccessTicket ticket) {

		if (ticket != null) {
			final Iterator<PFN> it = ticket.getPFNS().iterator();

			while (it.hasNext()) {
				final PFN pfn = it.next();

				if (ticket.type != AccessType.DENIED) {
					pfn.envelope = new XrootDEnvelope(ticket, pfn,
							SEUtils.getSE(pfn.seNumber));
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
					if (pfn.envelope.createEcryptedEnvelope)
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

	/**
	 * 
	 * load the RSA keys for envelope signature, keys are supposed to be in pem,
	 * and can be created with: openssl req -x509 -nodes -days 365 -newkey
	 * rsa:4096 -keyout lpriv.pem -out lpub.pem
	 * 
	 * @throws GeneralSecurityException
	 * @throws IOException
	 */

	private void loadKeys() throws GeneralSecurityException, IOException {

		Security.addProvider(new BouncyCastleProvider());

		AuthenPrivKey = (RSAPrivateKey) ((KeyPair) new PEMReader(
				new BufferedReader(new FileReader(AuthenPrivLocation)))
				.readObject()).getPrivate();
		AuthenPubKey = (RSAPublicKey) ((X509Certificate) new PEMReader(
				new BufferedReader(new FileReader(AuthenPubLocation)))
				.readObject()).getPublicKey();

		SEPrivKey = (RSAPrivateKey) ((KeyPair) new PEMReader(
				new BufferedReader(new FileReader(SEPrivLocation)))
				.readObject()).getPrivate();
		SEPubKey = (RSAPublicKey) ((X509Certificate) new PEMReader(
				new BufferedReader(new FileReader(SEPubLocation))).readObject())
				.getPublicKey();

	}

	private void signEnvelope(XrootDEnvelope envelope)
			throws NoSuchAlgorithmException, InvalidKeyException,
			SignatureException {

		// System.out.println("About to be signed: " +
		// envelope.getUnsignedEnvelope());

		long issued = System.currentTimeMillis() / 1000L;
		long expires = issued + 86400;

		String toBeSigned = envelope.getUnsignedEnvelope()
				+ "&issuer=JAuthenX&issued=" + issued + "&expires=" + expires
				+ "&hashord=" + XrootDEnvelope.hashord
				+ "-issuer-issued-expires-hashord";

		Signature signer = Signature.getInstance("SHA384withRSA");
		signer.initSign(AuthenPrivKey);
		signer.update(toBeSigned.getBytes());

		byte[] rawsignature = new byte[1024];
		rawsignature = signer.sign();

		envelope.setSignedEnvelope(toBeSigned + "&signature="
				+ String.valueOf(Base64.encode(rawsignature)));

		// System.out.println("We signed: " + envelope.getSignedEnvelope());

	}

	private void verifyEnvelope(XrootDEnvelope envelope, boolean selfSigned)
			throws NoSuchAlgorithmException, InvalidKeyException,
			SignatureException {

		// System.out.println("About to be signed: " +
		// envelope.getUnsignedEnvelope());

		long issued = System.currentTimeMillis() / 1000L;
		long expires = issued + 86400;

		String toBeSigned = envelope.getUnsignedEnvelope()
				+ "&issuer=JAuthenX&issued=" + issued + "&expires=" + expires
				+ "&hashord=" + XrootDEnvelope.hashord
				+ "-issuer-issued-expires-hashord";

		Signature signer = Signature.getInstance("SHA384withRSA");
		if (selfSigned){
			signer.initVerify(AuthenPubKey);
		} else {
			signer.initVerify(SEPubKey);
		}
		signer.update(toBeSigned.getBytes());

		byte[] rawsignature = new byte[1024];
		rawsignature = signer.sign();

		envelope.setSignedEnvelope(toBeSigned + "&signature="
				+ String.valueOf(Base64.encode(rawsignature)));

		// System.out.println("We signed: " + envelope.getSignedEnvelope());

	}

	private void encryptEnvelope(XrootDEnvelope envelope)
			throws GeneralSecurityException {

		// System.out.println("About to be encrypted: " +
		// envelope.getUnEncryptedEnvelope());

		if (authz == null)
			authz = new EncryptedAuthzToken(AuthenPrivKey, SEPubKey);

		envelope.setEncryptedEnvelope(authz.encrypt(envelope
				.getUnEncryptedEnvelope()));
		// System.out.println("We encrypted: " +
		// envelope.getEncryptedEnvelope());
	}

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
