package alien.api.taskQueue;

import alien.api.Request;
import alien.taskQueue.TaskQueueFakeUtils;

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
	private static final long serialVersionUID = 5934030877580023175L;
	private String jdl = null;
	
	/**
	 * @param jdl 
	 */
	public SubmitJob(String jdl){
		this.jdl = jdl;
	}
	
	@Override
	public void run() {
		System.out.println("received jdl submit...");
		TaskQueueFakeUtils.submitJob(this.jdl);
		
	}

	
	@Override
	public String toString() {
		return "Asked to submit JDL: "+this.jdl;
	}
}
