package alien.api.taskQueue;

import java.io.IOException;
import java.util.List;

import alien.api.Dispatcher;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobSigner;
import alien.taskQueue.JobSubmissionException;
import alien.user.JAKeyStore;

/**
 * Get the JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class TaskQueueApiUtils {

	
	/**
	 * @param states 
	 * @param users 
	 * @param sites 
	 * @param nodes 
	 * @param mjobs 
	 * @param jobid 
	 * @param limit 
	 * @return a PS listing
	 */
	public static List<Job> getPS(final List<String> states,final List<String> users,final List<String> sites,
			final List<String> nodes,final List<String> mjobs,final List<String> jobid, final int limit) {

		try {
			GetPS ps = (GetPS) Dispatcher.execute(new GetPS(states, users, sites, nodes, mjobs, jobid, limit), true);

			return ps.returnPS();
		} catch (IOException e) {
			System.out.println("Could get a PS listing: ");
			e.printStackTrace();
		}
		return null;

	}

	
	/**
	 * @param queueId 
	 * @return a JDL as String
	 */
	public static String getTraceLog(final int queueId) {

		try {
			GetTraceLog trace = (GetTraceLog) Dispatcher.execute(new GetTraceLog(queueId), true);

			return trace.getTraceLog();
		} catch (IOException e) {
			System.out.println("Could get not a TraceLog: ");
			e.printStackTrace();
		}
		return null;

	}
	
	/**
	 * @param queueId 
	 * @return a JDL as String
	 */
	public static String getJDL(final int queueId) {

		try {
			GetJDL jdl = (GetJDL) Dispatcher.execute(new GetJDL(queueId), true);

			return jdl.getJDL();
		} catch (IOException e) {
			System.out.println("Could get not a JDL: ");
			e.printStackTrace();
		}
		return null;

	}
	
	/**
	 * @return a Job
	 */
	public static Job getJob() {

		try {
			GetJob job = (GetJob) Dispatcher.execute(new GetJob(), true);

			return job.getJob();
		} catch (IOException e) {
			System.out.println("Could get not a JDL: ");
			e.printStackTrace();
		}
		return null;

	}

	/**
	 * Set a job's status
	 * 
	 * @param jobnumber
	 * @param status
	 */
	public static void setJobStatus(int jobnumber, String status) {

		try {
			Dispatcher.execute(new SetJobStatus(jobnumber, status), true);

		} catch (IOException e) {
			System.out.println("Could get not a Job's status: ");
			e.printStackTrace();
		}
	}

	/**
	 * Submit a job
	 * 
	 * @param jdl
	 * @param user
	 * @return int
	 * @throws JobSubmissionException
	 */
	public static int submitJob(String jdl, String user)
			throws JobSubmissionException {

		try {
			JDL ojdl = new JDL(jdl);
			SubmitJob j;
			j = new SubmitJob(JobSigner.signJob(JAKeyStore.clientCert,
					"User.cert", JAKeyStore.pass, user, ojdl, jdl));	// TODO : why was ojdl passed here ?

			Dispatcher.execute(j, true);
			return j.getJobID();

		} catch (Exception e) {
			System.out.println("Could not submit a JDL: ");
			e.printStackTrace();
		}
		return 0;
	}

}
