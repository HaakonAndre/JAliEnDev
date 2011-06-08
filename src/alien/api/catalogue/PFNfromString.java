package alien.api.catalogue;

import java.util.Set;
import java.util.UUID;

import alien.api.Cacheable;
import alien.api.Request;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;

/**
 * 
 * @author ron
 * @since Jun 03, 2011
 */
public class PFNfromString extends Request implements Cacheable{

	private static final long serialVersionUID = -3237006644358177225L;

	private final String sguid;

	private Set<PFN> pfns;

	/**
	 * Get PFNs by String
	 * @param sguid
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

	@Override
	public String getKey() {
		return this.sguid;
	}

	@Override
	public long getTimeout() {
		return 1000*60*5;
	}
}
