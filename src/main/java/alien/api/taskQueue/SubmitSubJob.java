package alien.api.taskQueue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.taskQueue.JDL;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

public class SubmitSubJob extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 7349968366381661013L;

	private final long masterId;
	private final List<JDL> jdls;
	private long jobID = 0;
	private final JDL masterJDL;

	/**
	 * @param user
	 * @param jdl
	 */
	public SubmitSubJob(final AliEnPrincipal account,long masterId, List<JDL> jdls, JDL masterJDL) {
		setRequestUser(account);
		this.masterId = masterId;
		this.jdls = jdls;
		this.masterJDL = masterJDL;
	}

	@Override
	public List<String> getArguments() {
			return Arrays.asList(String.valueOf(masterId), String.valueOf(jdls));

	}

	@Override
	public void run() {
		try {
			jobID = TaskQueueUtils.insertSubJob(getEffectiveRequester(),masterId, jdls, masterJDL);
		}
		catch (final IOException | SQLException ioe) {
			throw new IllegalArgumentException(ioe.getMessage());
		}
	}

	/**
	 * @return jobID
	 */
	public long getJobID() {
		return this.jobID;
	}

	@Override
	public String toString() {
		return "Asked to submit subjob for: " + this.masterId;
	}

}
