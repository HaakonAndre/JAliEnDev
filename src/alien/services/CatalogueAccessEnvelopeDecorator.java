package alien.services;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.RSAPrivateKey;
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

import alien.catalogue.PFN;
import alien.catalogue.access.CatalogueAccess;
import alien.catalogue.access.XrootDEnvelope;
import alien.se.SE;
import alien.se.SEUtils;
import alien.tsealedEnvelope.Base64;

/**
 * @author Steffen
 * @since Nov 14, 2010
 */
public class CatalogueAccessEnvelopeDecorator {

	/**
	 * @param ca
	 * @param sitename
	 * @param qos
	 * @param qosCount
	 * @param staticSEs
	 * @param exxSes
	 */
	public static void loadXrootDEnvelopesForCatalogueAccess(
			CatalogueAccess ca, String sitename, String qos, int qosCount,
			Set<SE> staticSEs, Set<SE> exxSes) {

		if (ca.getAccess() == CatalogueAccess.READ
				|| ca.getAccess() == CatalogueAccess.DELETE) {

			ca.loadPFNS();

			final Set<PFN> whereis = ca.getPFNS();

			// keep all replicas, even if the SE is reported as broken (though
			// it will go to the end of list)
			final List<PFN> sorted = SEUtils.sortBySite(whereis, sitename,
					false);

			// remove the replicas from these SEs, if specified
			if (exxSes != null && exxSes.size() > 0) {
				final Iterator<PFN> it = sorted.iterator();

				while (it.hasNext()) {
					final PFN pfn = it.next();

					final SE se = SEUtils.getSE(pfn.seNumber);

					if (exxSes.contains(se))
						it.remove();
				}
			}

			// getEnvelopesforPFNList(ca, sorted);
			if (sorted.size() > 0)
				ca.addEnvelope(new XrootDEnvelope(ca, sorted.get(0)));
		} else if (ca.getAccess() == CatalogueAccess.WRITE) {

			final List<SE> ses = new LinkedList<SE>();

			if (staticSEs != null)
				ses.addAll(staticSEs);

			filterSEs(ses, qos, exxSes);

			if (ses.size() >= qosCount) {
				final List<SE> dynamics = SEUtils.getClosestSEs(sitename);

				filterSEs(dynamics, qos, exxSes);

				final Iterator<SE> it = dynamics.iterator();

				while (ses.size() < qosCount && it.hasNext())
					ses.add(it.next());
			}

			getEnvelopesforSEList(ca, ses);
		}
	}

	private static final void filterSEs(final List<SE> ses, final String qos,
			final Set<SE> exxSes) {
		final Iterator<SE> it = ses.iterator();

		while (it.hasNext()) {
			final SE se = it.next();

			if (!se.qos.contains(qos) || exxSes.contains(se))
				it.remove();
		}
	}

	private static void getEnvelopesforSEList(final CatalogueAccess ca,
			final Collection<SE> ses) {
		for (final SE se : ses) {
			ca.addEnvelope(new XrootDEnvelope(ca, se, se.seioDaemons
					+ se.seStoragePath + ca.getGUID().toString()));
		}
	}

	public static ArrayList<String> signEnvelope(RSAPrivateKey AuthenPrivKey,
			Set<XrootDEnvelope> envelopes) throws NoSuchAlgorithmException,
			InvalidKeyException, SignatureException {

		ArrayList<String> signedEnvelopes = new ArrayList<String>();

	
		
		for (final XrootDEnvelope envelope : envelopes) {

			long created = System.currentTimeMillis() / 1000L;
			long expires = created + 86400;

			String toBeSigned = envelope.getUnsignedEnvelope()
					+ "&creator=JAuthenX&created=" + created + "&expires="
					+ expires + "&hashord=" + envelope.hashord
					+ "-creator-expires-hashord";

			Signature signer = Signature.getInstance("SHA384withRSA");
			signer.initSign(AuthenPrivKey);
			signer.update(toBeSigned.getBytes());
			// signer.update(digest);

			toBeSigned = toBeSigned + "&signature="
					+ Base64.encodeBytes(signer.sign());

			signedEnvelopes.add(toBeSigned.replace("&", "\\&"));
		}
	return signedEnvelopes;
	}
}
