package alien.api.taskQueue;

import java.util.List;

import alien.api.Request;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class GetPS extends Request {



	/**
	 * 
	 */
	private static final long serialVersionUID = -1486633687303580187L;

	/**
	 * 
	 */
	private List<Job> jobs;

	private final List<JobStatus> states;
	
	private final List<String> users;
	
	private final List<String> sites;
	
	private final List<String> nodes;
	
	private final List<String> mjobs;
	
	private final List<String> jobid;
		
	private final String orderByKey;
	
	private int  limit = 0;
	
	/**
	 * @param user 
	 * @param role 
	 * @param states 
	 * @param users 
	 * @param sites 
	 * @param nodes 
	 * @param mjobs 
	 * @param jobid 
	 * @param orderByKey 
	 * @param limit 
	 */
	public GetPS(final AliEnPrincipal user, final String role, final List<JobStatus> states,final List<String> users,final List<String> sites,
			final List<String> nodes,final List<String> mjobs,final List<String> jobid, final String orderByKey, final int limit){
		setRequestUser(user);
		setRoleRequest(role);
		this.states = states;
		this.users = users;
		this.sites = sites;
		this.nodes = nodes;
		this.mjobs = mjobs;
		this.jobid = jobid;
		this.limit = limit;
		this.orderByKey = orderByKey;
	}
	
	@Override
	public void run() {
		
		System.out.println("states: " + states);
		
		this.jobs = TaskQueueUtils.getPS(states, users, sites, nodes, mjobs, jobid, orderByKey, limit);
	}
	
	/**
	 * @return a JDL
	 */
	public List<Job> returnPS(){
		return this.jobs;
	}
	
	@Override
	public String toString() {
		return "Asked for PS :  reply is: "+this.jobs;
	}
}
