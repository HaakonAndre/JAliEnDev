package alien.api.taskQueue;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;

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
	 * @return a Job
	 */
	public static Job getJob() {

		try {
			GetJob job = (GetJob) Dispatcher.execute(new GetJob(), true);

			return job.getJob();
		} catch (IOException e) {
			System.out.println("Could not a JDL: ");
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
			System.out.println("Could not a JDL: ");
			e.printStackTrace();
		}
	}

	/**
	 * Submit a job
	 * 
	 * @param jdl
	 * @param user
	 * @return
	 * @throws JobSubmissionException
	 */
	public static int submitJob(String jdl, String user)
			throws JobSubmissionException {

		try {
			JDL ojdl = new JDL(jdl);
			SubmitJob j;
			j = new SubmitJob(JobSigner.signJob(JAKeyStore.clientCert,
					"User.cert", JAKeyStore.pass, user, jdl));	// TODO : why was ojdl passed here ?

			Dispatcher.execute(j, true);
			return j.getJobID();

		} catch (Exception e) {
			System.out.println("Could not submit a JDL: ");
			e.printStackTrace();
		}
		return 0;
	}

}
