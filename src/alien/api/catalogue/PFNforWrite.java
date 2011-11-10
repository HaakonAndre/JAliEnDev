package alien.api.catalogue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
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
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(PFNforWrite.class.getCanonicalName());

	private static final long serialVersionUID = 6219657670649893255L;

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
	 * @param role
	 * @param site
	 * @param lfn
	 * @param guid
	 * @param ses
	 * @param exses
	 * @param qosType
	 * @param qosCount
	 */
	public PFNforWrite(final AliEnPrincipal user, final String role, final String site, final LFN lfn, final GUID guid, final List<String> ses, final List<String> exses, final String qosType, final int qosCount) {
		setRequestUser(user);
		setRoleRequest(role);
		this.site = site;
		this.lfn = lfn;
		this.guid = guid;
		this.ses = ses;
		this.exses = exses;
		this.qosType = qosType;
		this.qosCount = qosCount;
	}

	/**
	 * Get PFNs to write
	 * 
	 * @param user
	 * @param role
	 * @param site
	 * @param guid
	 * @param ses
	 * @param exses
	 * @param qosType
	 * @param qosCount
	 */
	public PFNforWrite(final AliEnPrincipal user, final String role, final String site, final GUID guid, final List<String> ses, final List<String> exses, final String qosType, final int qosCount) {
		setRequestUser(user);
		setRoleRequest(role);
		this.site = site;
		this.guid = guid;
		this.ses = ses;
		this.exses = exses;
		this.qosType = qosType;
		this.qosCount = qosCount;
	}

	@Override
	public void run() {
		System.err.println("Request details : ----------------------\n"+guid+"\n ---------------------- \n "+lfn+" \n ---------------------- \n"+getEffectiveRequester());
		
		if (((ses == null) || ses.size()==0) && (qosType == null)) {
			final Set<String> defaultQos = LDAPHelper.checkLdapInformation("(objectClass=AliEnVOConfig)", "ou=Config,", "sedefaultQosandCount");

			if (defaultQos.isEmpty())
				logger.log(Level.WARNING, "No specification of storages and no default LDAP entry found.");

			String defQos = defaultQos.iterator().next();

			qosType = defQos.substring(0, defQos.indexOf('='));
			qosCount = Integer.parseInt(defQos.substring(defQos.indexOf('=') + 1));
		}
		
		List<SE> SEs = SEUtils.getSEs(ses);

		final List<SE> exSEs = SEUtils.getSEs(exses);

		int count = qosCount;
		if (ses != null)
			count += ses.size();

		pfns = new ArrayList<PFN>(count);

		// LFN setArchiveAnchor = null;

		if (lfn != null) {
			if (guid == null) {
				logger.log(Level.WARNING, "Cannot get PFNforWrite with guid=null, for lfn: " + lfn);
				return;
			}

			if (lfn.guid == null)
				lfn.guid = guid.guid;
			
			guid.lfnCache = new LinkedHashSet<LFN>(1);
			guid.lfnCache.add(lfn);
			guid.size = lfn.size;
			guid.md5 = lfn.md5;
		}
		else
			if (guid != null) {
				if (guid.lfnCache.size() > 0)
					lfn = guid.lfnCache.iterator().next();
			}

		// static list of specified SEs
		if (ses != null) {
			for (final SE se : SEs) {
				if (!se.canWrite(getEffectiveRequester())) {
					logger.log(Level.INFO, getEffectiveRequester()+" is not allowed to write to the explicitly requested SE "+se.seName);
					continue;
				}
				try {
					pfns.add(BookingTable.bookForWriting(getEffectiveRequester(), lfn, guid, null, 0, se));
				}
				catch (Exception e) {
					logger.log(Level.WARNING, "Error for the request on " + se.getName() + ", message", e.fillInStackTrace());
				}
			}
		}

		if (qosCount > 0) {
			if (exSEs != null)
				SEs.addAll(exSEs);
			
			SEs = SEUtils.getClosestSEs(site, SEs);
			final Iterator<SE> it = SEs.iterator();

			int counter = 0;
			while (counter < qosCount && it.hasNext()) {
				SE se = it.next();

				if (!se.canWrite(getEffectiveRequester()))
					continue;
				
				if(!se.isQosType(qosType))
					continue;

				try {
					pfns.add(BookingTable.bookForWriting(getEffectiveRequester(), lfn, guid, null, 0, se));
				}
				catch (Exception e) {
					logger.log(Level.WARNING, "Error requesting an envelope for " + se.getName(), e);
					e.printStackTrace();
					continue;
				}
				counter++;
			}

		}

		logger.log(Level.FINE, "Returning: "+this.toString());
		
		logger.log(Level.WARNING, "Returning: "+pfns);


		//
		// for (PFN pfn : pfns) {
		// if (pfn.ticket.envelope == null) {
		// System.err.println("Sorry ... Envelope is null!");
		// } else {
		// pfn.ticket.envelope.setArchiveAnchor(setArchiveAnchor);
		// try {
		// // we need to both encrypt and sign, the later is not
		// // automatic
		// XrootDEnvelopeSigner.signEnvelope(pfn.ticket.envelope);
		// } catch (SignatureException e) {
		// System.err
		// .println("Sorry ... Could not sign the envelope!");
		// } catch (InvalidKeyException e) {
		// System.err
		// .println("Sorry ... Could not sign the envelope!");
		// } catch (NoSuchAlgorithmException e) {
		// System.err
		// .println("Sorry ... Could not sign the envelope!");
		// }
		// String addEnv = pfn.ticket.envelope.getSignedEnvelope();
		//
		// // drop the following once LDAP schema is updated and version
		// // number properly on
		// if (!"alice::cern::setest".equals(SEUtils.getSE(pfn.seNumber)
		// .getName().toLowerCase())) {
		// if (SEUtils.getSE(pfn.seNumber).needsEncryptedEnvelope) {
		// addEnv += "\\&oldEnvelope="
		// + pfn.ticket.envelope.getEncryptedEnvelope();
		// System.out.println("Creating ticket (encrypted): "
		// + pfn.ticket.envelope.getUnEncryptedEnvelope());
		// }
		// }
		//
		// }
		// }

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
			return "Asked for write: " + this.lfn + " (" + this.site + "," + this.qosType + "," + this.qosCount + "," + this.ses + "," + this.exses + "), reply is: " + this.pfns;
		
		if (guid != null)
			return "Asked for write: " + this.guid + " (" + this.site + "," + this.qosType + "," + this.qosCount + "," + this.ses + "," + this.exses + "), reply is: " + this.pfns;
		
		return "Asked for write: unspecified target!";
	}
}
