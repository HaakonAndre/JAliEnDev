package alien.ui.api;

import java.util.Set;
import java.util.UUID;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.ui.Request;

/**
 * Get the PFN object for String
 *
 * @author ron
 * @since Jun 04, 2011
 */
public class PFNfromString extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3237006644358177225L;

	private final String sguid;

	private Set<PFN> pfns;

	/**
	 * @param sguid
	 * @param evenIfDoesNotExist
	 */
	public PFNfromString(final String sguid) {
		this.sguid = sguid;
	}

	@Override
	public void run() {
		this.pfns = GUIDUtils.getGUID(UUID.fromString(sguid)).getPFNs();
	}

	/**
	 * @return the requested PFNs
	 */
	public Set<PFN> getPFNs() {
		return this.pfns;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.sguid + " , reply is:\n" + this.pfns;
	}
}
