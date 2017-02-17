package alien.api.taskQueue;

import java.io.IOException;

import alien.api.Request;
import alien.api.ServerException;
import alien.taskQueue.JDL;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class SubmitJob extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7349968366381661013L;
	
	
	private final JDL jdl;
	private long jobID = 0;

	/**
	 * @param user 
	 * @param role 
	 * @param jdl
	 */
	public SubmitJob(final AliEnPrincipal user, final String role, final JDL jdl) {
		setRequestUser(user);
		setRoleRequest(role);
		this.jdl = jdl;
	}

	@Override
	public void run(){
		try{
			jobID = TaskQueueUtils.submit(jdl, getEffectiveRequester(), getRoleRequest());
		}
		catch (IOException ioe){
//			System.out.println("ex: " + ioe.getMessage());
			setException(new ServerException(ioe.getMessage(), ioe));
		}
	}

	/**
	 * @return jobID
	 */
	public long getJobID(){
		System.out.println("job received ID:" + this.jobID);

		return this.jobID;
	}


	@Override
	public String toString() {
		return "Asked to submit JDL: " + this.jdl;
	}
}
