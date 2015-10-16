package alien.api.taskQueue;

import alien.api.Request;
import alien.quotas.FileQuota;
import alien.quotas.QuotaUtilities;
import alien.user.AliEnPrincipal;

/**
 * Get the file quota for a given user
 */
public class GetFileQuota extends Request {

	private static final long serialVersionUID = -5786988633059376978L;
	private final String username;
	private FileQuota q;

	/**
	 * @param user
	 */
	public GetFileQuota(final AliEnPrincipal user) {
		this.username = user.getName();
	}

	@Override
	public void run() {
		this.q = QuotaUtilities.getFileQuota(this.username);
	}

	/**
	 * @return the file quota for this user
	 */
	public FileQuota getFileQuota() {
		return this.q;
	}
}
