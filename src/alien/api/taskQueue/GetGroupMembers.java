package alien.api.taskQueue;

import java.util.Set;

import alien.api.Request;
import alien.user.AliEnPrincipal;

/**
 * Get the member accounts of a given group
 */
public class GetGroupMembers extends Request {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3645332494815939574L;
	private Set<String> members;
	private final String groupname;

	/**
	 * @param user
	 * @param group
	 */
	public GetGroupMembers(final AliEnPrincipal user, final String group) {
		this.groupname = group;
	}

	@Override
	public void run() {
		// this.q = QuotaUtilities.getFileQuota( this.username );
		this.members = AliEnPrincipal.getRoleMembers(this.groupname);
	}

	/**
	 * @return members of the requested group
	 */
	public Set<String> getMembers() {
		return this.members;
	}
}
