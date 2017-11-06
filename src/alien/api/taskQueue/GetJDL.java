package alien.api.taskQueue;

import alien.api.Request;
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
	/**
	 *
	 */
	private static final long serialVersionUID = 5445861914172537974L;

	private final int queueId;

	private String jdl;

	/**
	 * @param user
	 * @param queueId
	 */
	public GetJDL(final AliEnPrincipal user, final int queueId) {
		setRequestUser(user);
		this.queueId = queueId;
	}

	@Override
	public void run() {
		this.jdl = Job.sanitizeJDL(TaskQueueUtils.getJDL(queueId));
	}

	/**
	 * @return a JDL
	 */
	public String getJDL() {
		return this.jdl;
	}

	@Override
	public String toString() {
		return "Asked for JDL :  reply is: " + this.jdl;
	}
}
