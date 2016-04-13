package alien.api.taskQueue;

import java.util.ArrayList;
import java.util.List;

import alien.api.Request;
import alien.taskQueue.Job;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Get a JDL object
 *
 * @author ron
 * @since Oct 28, 2011
 */
public class GetJobs extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 385621115781357192L;

	private List<Job> jobs;

	private final List<Long> queueIds;

	/**
	 * @param user
	 * @param role
	 * @param queueIds
	 */
	public GetJobs(final AliEnPrincipal user, final String role, final List<Long> queueIds) {
		setRequestUser(user);
		setRoleRequest(role);
		this.queueIds = queueIds;
	}

	@Override
	public void run() {
		jobs = new ArrayList<>(queueIds.size());
		for (final Long qId : queueIds)
			jobs.add(TaskQueueUtils.getJob(qId.longValue()));
	}

	/**
	 * @return the Jobs
	 */
	public List<Job> getJobs() {
		return this.jobs;
	}

	@Override
	public String toString() {
		return "Asked for Jobs :  reply is: " + this.jobs;
	}
}
