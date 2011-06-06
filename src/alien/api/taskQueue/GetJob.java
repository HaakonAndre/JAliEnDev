package alien.api.taskQueue;

import alien.api.Request;
import alien.taskQueue.TaskQueueFakeUtils;
import alien.taskQueue.Job;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class GetJob extends Request {



	/**
	 * 
	 */
	private static final long serialVersionUID = -3575992501982425989L;
	private Job job;
	
	/**
	 */
	public GetJob(){
	}
	
	@Override
	public void run() {
		this.job = TaskQueueFakeUtils.getJob();
	}
	
	/**
	 * @return a JDL
	 */
	public Job getJob(){
		return this.job;
	}
	
	@Override
	public String toString() {
		return "Asked for JDL :  reply is: "+this.job;
	}
}
