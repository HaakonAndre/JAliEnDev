package alien.api.taskQueue;

import alien.api.Request;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class SetJobStatus extends Request {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6330031807464568209L;
	private final int jobnumber;
	private final JobStatus status;
	
	/**
	 * @param jobnumber 
	 * @param status 
	 */
	public SetJobStatus(int jobnumber, JobStatus status){
		this.jobnumber = jobnumber;
		this.status = status;
	}
	
	@Override
	public void run() {
		TaskQueueUtils.setJobStatus(this.jobnumber, this.status);
	}

	
	@Override
	public String toString() {
		return "Asked to set job ["+this.jobnumber+"] to status: "+this.status;
	}
}
