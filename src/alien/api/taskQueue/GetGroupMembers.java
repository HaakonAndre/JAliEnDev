package alien.api.taskQueue;

import alien.api.Request;
import alien.user.AliEnPrincipal;

import java.util.Set;

public class GetGroupMembers extends Request {
	private String username;
	private Set<String> members;
	private String groupname;
	
	/**
	 * @param user 
	 * @param role 
	 * @param queueId
	 */
	public GetGroupMembers(final AliEnPrincipal user, String group) {
		this.groupname = group;
	}
	
	@Override
	public void run() {
		//this.q = QuotaUtilities.getFileQuota( this.username );
		this.members = AliEnPrincipal.getRoleMembers( this.groupname );
	}
	
	public Set<String> getMembers(){
		return this.members;
	}
}
