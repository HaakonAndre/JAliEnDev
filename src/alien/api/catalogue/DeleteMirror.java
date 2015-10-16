package alien.api.catalogue;

import java.util.UUID;

import alien.api.Request;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * delete a mirror
 */
public class DeleteMirror extends Request {

	private static final long serialVersionUID = 5308609993726210313L;
	private final String path;
	private final String se;
	private final boolean isGuid;
	private int result;

	/**
	 * @param user
	 * @param role
	 * @param fpath
	 * @param isGuid
	 * @param se
	 */
	public DeleteMirror(final AliEnPrincipal user, final String role, final String fpath, final boolean isGuid, final String se) {
		this.path = fpath;
		this.isGuid = isGuid;
		this.se = se;
	}

	@Override
	public void run() {
		if (isGuid && !GUIDUtils.isValidGUID(this.path)) {
			this.result = -1; // invalid GUID
			return;
		}
		final SE s = SEUtils.getSE(this.se);
		if (s == null) {
			this.result = -2; // failed to get SE
			return;
		}
		GUID g;
		if (this.isGuid)
			g = GUIDUtils.getGUID(UUID.fromString(path), false);
		else {
			final LFN lfn = LFNUtils.getLFN(path, true);
			g = GUIDUtils.getGUID(lfn);
		}
		// Here check authorization for delete mirror procedure
		if (!AuthorizationChecker.isOwner(g, this.getEffectiveRequester())) {
			this.result = -3; // not authorized
			return;
		}

		final String pfn = g.removePFN(s, true);
		this.result = (pfn != null ? 0 : -4); // failed for different reason
	}

	/**
	 * @return exit code
	 */
	public int getResult() {
		return this.result;
	}

}