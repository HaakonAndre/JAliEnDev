package alien.api.taskQueue;

import java.util.HashMap;

import alien.api.Request;
import alien.taskQueue.JobBroker;
import alien.user.AliEnPrincipal;

/**
 * Get the number of waiting jobs fitting site requirements
 * 
 * @author mmmartin
 * @since March 1, 2017
 */
public class GetNumberWaitingJobs extends Request {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5445861912342537975L;

	private HashMap<String, Object> match;
	private HashMap<String, Object> matchRequest;

	/**
	 * @param user
	 * @param role
	 * @param siteMap
	 */
	public GetNumberWaitingJobs(final AliEnPrincipal user, final String role, final HashMap<String, Object> siteMap) {
		setRequestUser(user);
		setRoleRequest(role);
		this.matchRequest = siteMap;
	}

	@Override
	public void run() {
		this.match = JobBroker.getNumberWaitingForSite(matchRequest);
	}

	/**
	 * @return number of jobs
	 */
	public Integer getNumberJobsWaitingForSite() {
		if (((Integer) match.get("Code")).intValue() == 1) {
			return (match.containsKey("counter") ? (Integer) match.get("counter") : Integer.valueOf(0));
		}

		return Integer.valueOf(0);
	}

	@Override
	public String toString() {
		return "Asked for number of waiting jobs for site, reply is: " + this.match;
	}
}
