package alien.api.taskQueue;

import alien.api.Request;
import alien.taskQueue.JobSubmissionException;
import alien.taskQueue.TaskQueueFakeUtils;
import alien.taskQueue.TaskQueueUtils;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class KillJob extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7349968366381661013L;
	
	
	private final int queueId;
	
	private boolean wasKilled = false;

	/**
	 * @param queueId
	 */
	public KillJob(final int queueId) {
		this.queueId = queueId;
	}


	public void run() {
		this.wasKilled = TaskQueueUtils.killJob(queueId);
	}

	/**
	 * @return success of the kill
	 */
	public boolean wasKilled(){
		return this.wasKilled();
	}

	public String toString() {
		return "Asked to kill job: " + this.queueId;
	}
}
