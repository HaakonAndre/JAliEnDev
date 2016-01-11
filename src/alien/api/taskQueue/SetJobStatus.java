package alien.api.taskQueue;

import java.util.HashMap;

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
	private final HashMap<String,Object> extrafields;
	
	/**
	 * @param jobnumber 
	 * @param status 
	 */
	public SetJobStatus(int jobnumber, JobStatus status){
		this.jobnumber = jobnumber;
		this.status = status;
		this.extrafields = null;
	}
	
	/**
	 * @param jobnumber 
	 * @param status 
	 * @param extrafields 
	*/
	public SetJobStatus(int jobnumber, JobStatus status, HashMap<String,Object> extrafields){
		this.jobnumber = jobnumber;
		this.status = status;
		this.extrafields = extrafields;
	}
	
	
	@Override
	public void run() {
		TaskQueueUtils.setJobStatus(this.jobnumber, this.status, null, this.extrafields);
	}

	
	@Override
	public String toString() {
		return "Asked to set job ["+this.jobnumber+"] to status: "+this.status;
	}
}
