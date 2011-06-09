package alien.api.catalogue;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import alien.api.Request;
import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.LDAPHelper;

/**
 * 
 * @author ron
 * @since Jun 03, 2011
 */
public class PFNforWrite extends Request {

	private static final long serialVersionUID = 6219657670649893255L;

	private AliEnPrincipal user = null;
	private String site = null;
	private LFN lfn = null;
	private GUID guid = null;
	private List<String> ses = null;
	private List<String> exses = null;
	private String qosType = null;
	private int qosCount = 0;

	private List<PFN> pfns = null;

	/**
	 * Get PFNs to write
	 * 
	 * @param user
	 * @param site
	 * @param lfn
	 * @param ses
	 * @param exses
	 * @param qosType
	 * @param qosCount
	 */
	public PFNforWrite(AliEnPrincipal user, String site, LFN lfn,
			List<String> ses, List<String> exses, String qosType, int qosCount) {
		this.user = user;
		this.site = site;
		this.lfn = lfn;
		this.ses = ses;
		this.exses = exses;
		this.qosType = qosType;
		this.qosCount = qosCount;
	}

	/**
	 * Get PFNs to write
	 * 
	 * @param user
	 * @param site
	 * @param guid
	 * @param ses
	 * @param exses
	 * @param qosType
	 * @param qosCount
	 */
	public PFNforWrite(AliEnPrincipal user, String site, GUID guid,
			List<String> ses, List<String> exses, String qosType, int qosCount) {
		this.user = user;
		this.site = site;
		this.guid = guid;
		this.ses = ses;
		this.exses = exses;
		this.qosType = qosType;
		this.qosCount = qosCount;
	}

	@Override
	public void run() {

		
		
		if ((ses == null) && (qosType == null)) {

			final Set<String> defaultQos = LDAPHelper.checkLdapInformation(
					"(objectClass=AliEnVOConfig)", "ou=Config,",
					"sedefaultQosandCount");

			if (defaultQos.isEmpty())
				System.err
						.println("No specification of storages and no default LDAP entry found.");

			String defQos = defaultQos.iterator().next();


			qosType = defQos.substring(0, defQos.indexOf('='));
			qosCount = Integer
					.parseInt(defQos.substring(defQos.indexOf('=') + 1));


		}

		
		List<SE> SEs = SEUtils.getSEs(ses);

		List<SE> exSEs = SEUtils.getSEs(exses);

		int count = qosCount;
		if (ses != null)
			count += ses.size();

		pfns = new ArrayList<PFN>(count);

		LFN setArchiveAnchor = null;

		if (lfn.guid == null)
				guid = GUIDUtils.createGuid();
		else
			guid = GUIDUtils.getGUID(lfn.guid, true);
		lfn.guid = guid.guid;
		guid.lfnCache = new LinkedHashSet<LFN>(1);
		guid.lfnCache.add(lfn);
		guid.size = lfn.size;
		guid.md5 = lfn.md5;

		// statis list of specified SEs
		if (ses != null) {
			for (SE se : SEs) {


				if (!se.canWrite(user)) {
					System.err
							.println("You are not allowed to write to this SE.");
					continue;
				}
				try {
					pfns.add(BookingTable.bookForWriting(user, lfn, guid, null,
							0, se));
				} catch (Exception e) {
					System.out.println("Error for the request on "
							+ se.getName() + ", message: " + e);
				}
			}
		}

		if (qosCount > 0) {
			if(exSEs!=null)
				SEs.addAll(exSEs);
			SEs = SEUtils.getClosestSEs(site, SEs);
			final Iterator<SE> it = SEs.iterator();

			int counter = 0;
			while (counter < qosCount && it.hasNext()) {
				SE se = it.next();

				if (!se.canWrite(user))
					continue;

				try {
					pfns.add(BookingTable.bookForWriting(user, lfn, guid, null,
							0, se));
				} catch (Exception e) {
					System.out.println("Error for the request on "
							+ se.getName() + ", message: " + e);
					continue;
				}
				counter++;
			}

		}
//
//		for (PFN pfn : pfns) {
//			if (pfn.ticket.envelope == null) {
//				System.err.println("Sorry ... Envelope is null!");
//			} else {
//				pfn.ticket.envelope.setArchiveAnchor(setArchiveAnchor);
//				try {
//					// we need to both encrypt and sign, the later is not
//					// automatic
//					XrootDEnvelopeSigner.signEnvelope(pfn.ticket.envelope);
//				} catch (SignatureException e) {
//					System.err
//							.println("Sorry ... Could not sign the envelope!");
//				} catch (InvalidKeyException e) {
//					System.err
//							.println("Sorry ... Could not sign the envelope!");
//				} catch (NoSuchAlgorithmException e) {
//					System.err
//							.println("Sorry ... Could not sign the envelope!");
//				}
//				String addEnv = pfn.ticket.envelope.getSignedEnvelope();
//
//				// drop the following once LDAP schema is updated and version
//				// number properly on
//				if (!"alice::cern::setest".equals(SEUtils.getSE(pfn.seNumber)
//						.getName().toLowerCase())) {
//					if (SEUtils.getSE(pfn.seNumber).needsEncryptedEnvelope) {
//						addEnv += "\\&oldEnvelope="
//								+ pfn.ticket.envelope.getEncryptedEnvelope();
//						System.out.println("Creating ticket (encrypted): "
//								+ pfn.ticket.envelope.getUnEncryptedEnvelope());
//					}
//				}
//
//			}
//		}

	}

	/**
	 * @return PFNs to write on
	 */
	public List<PFN> getPFNs() {

		return pfns;
	}

	@Override
	public String toString() {
		if (lfn != null)
			return "Asked for write: " + this.lfn + " (" + this.site + ","+ this.qosType+ ","+this.qosCount+ ","+ this.ses+ ","+ this.exses+"), reply is: "
					+ this.pfns;
		else if (guid != null)
			return "Asked for write: " + this.guid + " (" + this.site + ","+ this.qosType+ ","+this.qosCount+ ","+ this.ses+ ","+ this.exses+"), reply is: "
					+ this.pfns;
		else
			return "Asked for write: unspecified target!";
	}
}
