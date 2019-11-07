package alien.api.catalogue;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.CatalogEntity;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 *
 * @author ron
 * @since Jun 03, 2011
 */
public class PFNforReadOrDel extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = 6219657670649893255L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(PFNforReadOrDel.class.getCanonicalName());

	private final AccessType access;

	private final String site;
	private final CatalogEntity entity;

	// don't remove this guid, if the guid is not send with the pfn to the
	// client, the thing goes nuts!
	// private GUID guid = null;

	private final List<String> ses;
	private final List<String> exses;

	private List<PFN> pfns = null;

	/**
	 * Get PFNs to read
	 *
	 * @param user
	 * @param site
	 * @param access
	 * @param entity
	 * @param ses
	 * @param exses
	 */
	public PFNforReadOrDel(final AliEnPrincipal user, final String site, final AccessType access, final CatalogEntity entity, final List<String> ses, final List<String> exses) {
		setRequestUser(user);
		this.site = site;
		this.entity = entity;
		this.access = access;
		this.ses = ses;
		this.exses = exses;
	}

	@Override
	public void run() {
		final GUID guid = entity instanceof GUID ? (GUID) entity : GUIDUtils.getGUID(((LFN) entity));

		boolean setArchiveAnchor = false;

		pfns = new LinkedList<>();

		if (guid.getPFNs() != null && !guid.getPFNs().isEmpty()) {
			try {
				for (final PFN pfn : guid.getPFNs()) {
					final UUID archiveLinkedTo = pfn.retrieveArchiveLinkedGUID();

					if (archiveLinkedTo != null) {
						final GUID archiveguid = GUIDUtils.getGUID(archiveLinkedTo, false);

						setArchiveAnchor = true;

						if (!AuthorizationChecker.canRead(archiveguid, getEffectiveRequester())) {
							logger.log(Level.WARNING, "Access refused because: Not allowed to read sub-archive");
							continue;
						}

						if (archiveguid.getPFNs() == null) {
							logger.log(Level.WARNING, "Failed to get DB connection when asked for PFNs");
							continue;
						}
						if (archiveguid.getPFNs().isEmpty()) {
							logger.log(Level.WARNING, "Failed to get any PFN for the archive");
							continue;
						}
						for (final PFN apfn : archiveguid.getPFNs()) {
							final String reason = AuthorizationFactory.fillAccess(getEffectiveRequester(), apfn, access);

							if (reason != null) {
								logger.log(Level.WARNING, "Access refused to " + apfn.getPFN() + " because: " + reason);
								continue;
							}

							logger.log(Level.FINE, "We have an envelope candidate: " + apfn.getPFN());

							pfns.add(apfn);
						}
					}
					else {
						final String reason = AuthorizationFactory.fillAccess(getEffectiveRequester(), pfn, access);

						if (reason != null) {
							logger.log(Level.WARNING, "Access refused because: " + reason);
							continue;
						}

						pfns.add(pfn);
					}
				}

			}
			catch (final Exception e) {
				logger.log(Level.SEVERE, "WE HAVE AN Exception", e);
			}

			if (pfns.size() > 0) {
				pfns = SEUtils.sortBySiteSpecifySEs(pfns, site, true, SEUtils.getSEs(ses), SEUtils.getSEs(exses), false);

				if (setArchiveAnchor)
					for (final PFN pfn : pfns)
						if (pfn.ticket.envelope == null)
							logger.log(Level.WARNING, "Can't set archive anchor on " + pfn.pfn + " since the envelope is null");
						else if (entity instanceof LFN)
							pfn.ticket.envelope.setArchiveAnchor((LFN) entity);
			}
			else
				logger.log(Level.WARNING, "Sorry ... No PFN to make an envelope for!");
		}
		else
			logger.log(Level.WARNING, "Sorry ... No PFNs for the file's GUID!");

		if (pfns.size() < 1)
			logger.log(Level.WARNING, "Sorry ... No PFNs for the file's GUID!");
	}

	/**
	 * @return PFNs to read from
	 */
	public List<PFN> getPFNs() {
		return pfns;
	}

	@Override
	public String toString() {
		return "Asked for read/delete: " + this.entity + "\n" + "reply is: " + this.pfns;
	}
}
