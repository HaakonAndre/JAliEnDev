package alien.api.taskQueue;

import alien.api.Request;
import alien.taskQueue.JDL;
import alien.taskQueue.TaskQueueUtils;

/**
 * Add a results JDL to DB
 *
 * @author mstoretv
 * @since Jul 15, 2019
 */
public class AddResultsJDL extends Request {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 *
	 */
	private final JDL jdl;
	private final long queueId;

	/**
	 * @param jobnumber
	 * @param status
	 */
	public AddResultsJDL(final JDL jdl, final long queueId) {
		this.jdl = jdl;
		this.queueId = queueId;
	}

	@Override
	public void run() {
		TaskQueueUtils.addResultsJdl(this.jdl, this.queueId);
	}

	@Override
	public String toString() {
		return "Adding the following results JDL for queueID: [" + this.queueId + "] :" + jdl.toString();
	}
}