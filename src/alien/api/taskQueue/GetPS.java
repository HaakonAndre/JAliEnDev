package alien.api.taskQueue;

import java.util.Collection;
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

	private final Collection<JobStatus> states;
	
	private final Collection<String> users;
	
	private final Collection<String> sites;
	
	private final Collection<String> nodes;
	
	private final Collection<Integer> mjobs;
	
	private final Collection<Integer> jobid;
		
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
	public GetPS(final AliEnPrincipal user, final String role, final Collection<JobStatus> states,final Collection<String> users,final Collection<String> sites,
			final Collection<String> nodes,final Collection<Integer> mjobs,final Collection<Integer> jobid, final String orderByKey, final int limit){
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
