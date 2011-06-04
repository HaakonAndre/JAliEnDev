package alien.ui.api;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.se.SE;
import alien.se.SEUtils;
import alien.services.XrootDEnvelopeSigner;
import alien.ui.Request;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * Get the LFN object for this path
 * 
 * @author ron
 * @since Jun 04, 2011
 */
public class PFNforReadOrDel extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6219657670649893255L;

	private AccessType access = null;

	private AliEnPrincipal user = null;
	private String site = null;
	private LFN lfn = null;
	private GUID guid = null;
	private List<String> ses = null;
	private List<String> exses = null;

	private List<PFN> pfns = null;

	/**
	 * @param path
	 * @param evenIfDoesNotExist
	 */
	public PFNforReadOrDel(AliEnPrincipal user, String site, AccessType access,
			LFN lfn, List<String> ses, List<String> exses) {
		this.user = user;
		this.site = site;
		this.lfn = lfn;
		this.access = access;
		this.ses = ses;
		this.exses = exses;
	}

	/**
	 * @param path
	 * @param evenIfDoesNotExist
	 */
	public PFNforReadOrDel(AliEnPrincipal user, String site, AccessType access,
			GUID guid, List<String> ses, List<String> exses) {
		this.user = user;
		this.site = site;
		this.guid = guid;
		this.access = access;
		this.ses = ses;
		this.exses = exses;
	}

	@Override
	public void run() {

		if (guid == null)
			guid = GUIDUtils.getGUID(lfn.guid);

		LFN setArchiveAnchor = null;

		PFN readpfn = null;

		pfns = SEUtils.sortBySiteSpecifySEs(guid.getPFNs(), site, true,
				SEUtils.getSEs(ses), SEUtils.getSEs(exses));

		try {
			for (PFN pfn : pfns) {

				System.out.println("Asking read for " + user.getName() + " to "
						+ guid.guid);
				String reason = AuthorizationFactory.fillAccess(user, pfn,
						access);

				if (reason != null) {
					System.err.println("Access refused because: " + reason);
					continue;
				}
				UUID archiveLinkedTo = pfn.retrieveArchiveLinkedGUID();
				if (archiveLinkedTo != null) {
					GUID archiveguid = GUIDUtils
							.getGUID(archiveLinkedTo, false);
					setArchiveAnchor = lfn;
					List<PFN> apfns = SEUtils.sortBySiteSpecifySEs(
							GUIDUtils.getGUID(pfn.retrieveArchiveLinkedGUID())
									.getPFNs(), site, true,
							SEUtils.getSEs(ses), SEUtils.getSEs(exses));
					if (!AuthorizationChecker.canRead(archiveguid, user)) {
						System.err
								.println("Access refused because: Not allowed to read sub-archive");
						continue;
					}

					for (PFN apfn : apfns) {

						reason = AuthorizationFactory.fillAccess(user, apfn,
								access);

						if (reason != null) {
							System.err.println("Access refused because: "
									+ reason);
							continue;
						}
						System.out.println("We have an evenlope candidate: "
								+ apfn.getPFN());
						readpfn = apfn;
						break;

					}
				} else {
					readpfn = pfn;
				}
				break;

			}

			pfns.clear();
			pfns.add(readpfn);

		} catch (Exception e) {
			System.out.println("WE HAVE AN Exception: " + e.toString());
		}

		for (PFN pfn : pfns) {
			if (pfn.ticket.envelope == null) {
				System.err.println("Sorry ... Envelope is null!");
			} else {
				pfn.ticket.envelope.setArchiveAnchor(setArchiveAnchor);
				try {
					// we need to both encrypt and sign, the later is not
					// automatic
					XrootDEnvelopeSigner.signEnvelope(pfn.ticket.envelope);
				} catch (SignatureException e) {
					System.err
							.println("Sorry ... Could not sign the envelope!");
				} catch (InvalidKeyException e) {
					System.err
							.println("Sorry ... Could not sign the envelope!");
				} catch (NoSuchAlgorithmException e) {
					System.err
							.println("Sorry ... Could not sign the envelope!");
				}
			}
		}

	}

	public List<PFN> getPFNs() {
		return pfns;
	}

	@Override
	public String toString() {
		if (lfn != null)
			return "Asked for read/delete: " + this.lfn + " ("
					+ "), reply is: " + this.pfns;
		else if (guid != null)
			return "Asked for read/delete: " + this.guid + " ("
					+ "), reply is: " + this.pfns;
		else
			return "Asked for write: unspecified target!";
	}
}
