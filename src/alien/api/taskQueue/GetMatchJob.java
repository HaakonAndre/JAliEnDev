package alien.api.taskQueue;

import java.util.HashMap;

import alien.api.Request;
import alien.taskQueue.JobBroker;
import alien.user.AliEnPrincipal;

/**
 * Get a job matching a JobAgent request
 * 
 * @author mmmartin 
 * @since Apr 1, 2015
 */
public class GetMatchJob extends Request {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5445861914172537975L;

	private HashMap<String, Object> match;
	private HashMap<String, Object> matchRequest;
	
	/**
	 * @param user 
	 * @param role 
	 * @param siteMap 
	 */
	public GetMatchJob(final AliEnPrincipal user, final String role, final HashMap<String, Object> siteMap){
		setRequestUser(user);
		setRoleRequest(role);
		this.matchRequest = siteMap;
	}
	
	@Override
	public void run() {
		this.match = JobBroker.getMatchJob(matchRequest);
	}
	
	/**
	 * @return a matched job
	 */
	public HashMap<String, Object> getMatchJob(){
		return this.match;
	}
	
	@Override
	public String toString() {
		return "Asked for a matching job, reply is: "+this.match;
	}
}
