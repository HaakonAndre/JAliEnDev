package alien.api.catalogue;

import java.util.UUID;

import alien.api.Request;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.user.AliEnPrincipal;

/**
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
	 * @param user 
	 * @param role 
	 * @param sguid
	 * @param evenIfDoesNotExist
	 */
	public GUIDfromString(final AliEnPrincipal user, final String role, final String sguid, final boolean evenIfDoesNotExist) {
		setRequestUser(user);
		setRoleRequest(role);
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
