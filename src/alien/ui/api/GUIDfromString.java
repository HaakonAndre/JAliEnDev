package alien.ui.api;

import java.util.UUID;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.ui.Request;

/**
 * Get the GUID object for String
 *
 * @author ron
 * @since Jun 03, 2011
 */
public class GUIDfromString extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3670065132137151044L;
	private final String sguid;
	private final boolean evenIfDoesNotExist;

	private GUID guid;

	/**
	 * @param sguid
	 * @param evenIfDoesNotExist
	 */
	public GUIDfromString(final String sguid, final boolean evenIfDoesNotExist) {
		this.sguid = sguid;
		this.evenIfDoesNotExist = evenIfDoesNotExist;
	}

	@Override
	public void run() {
		this.guid = GUIDUtils.getGUID(UUID.fromString(sguid),
				evenIfDoesNotExist);
	}

	/**
	 * @return the requested GUID
	 */
	public GUID getGUID() {
		return this.guid;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.sguid + " (" + this.evenIfDoesNotExist
				+ "), reply is:\n" + this.guid;
	}
}
