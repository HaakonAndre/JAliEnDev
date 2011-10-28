package alien.api.taskQueue;

import alien.api.Request;
import alien.taskQueue.TaskQueueFakeUtils;
import alien.taskQueue.Job;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Oct 26, 2011
 */
public class GetJDL extends Request {



	private final int queueId;
	
	private String jdl;
	
	/**
	 * @param user 
	 * @param role 
	 * @param queueId 
	 */
	public GetJDL(final AliEnPrincipal user, final String role, final int queueId){
		setRequestUser(user);
		setRoleRequest(role);
		this.queueId = queueId;
	}
	
	@Override
	public void run() {
		this.jdl = TaskQueueUtils.getJDL(queueId);
	}
	
	/**
	 * @return a JDL
	 */
	public String getJDL(){
		return this.jdl;
	}
	
	@Override
	public String toString() {
		return "Asked for JDL :  reply is: "+this.jdl;
	}
}
