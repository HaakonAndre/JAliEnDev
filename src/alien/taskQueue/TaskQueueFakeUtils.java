package alien.taskQueue;

import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.HashMap;

import alien.user.AliEnPrincipal;
import alien.user.JAKeyStore;

import javax.security.cert.X509Certificate;

import lazyj.Utils;

/**
 * @author ron
 * @since Jun 05, 2011
 */
public class TaskQueueFakeUtils {

	private static int jobcounter = (int) (System.currentTimeMillis() / 1000L);

	private static HashMap<Integer, Job> queue = new HashMap<Integer, Job>();

	/**
	 * @return a job
	 */
	public static Job getJob() {

		// Job j = fakeJob();
		if (queue.containsKey(jobcounter) && queue.get(jobcounter) != null) {
			if (getJobStatus(jobcounter).equals("WAITING")) {
				Job j = queue.get(jobcounter);
				System.out.println("submitting job: " + j.jdl);
				setJobStatus(j.queueId, "ASSIGNED");
				return j;
			}
		}
		return null;
	}

	/**
	 * @return fake job
	 */
	public static Job fakeJob() {
		Job j = new Job();
		jobcounter++;
		j.queueId = jobcounter;

		j.status = "WAITING";

		j.jdl = Utils.readFile("/tmp/myFirst.jdl");

		j.site = "";
		j.started = 0;
		queue.put(jobcounter, j);
		return j;
	}

	/**
	 * Submit a job
	 * 
	 * @param jdl
	 * @param user
	 * @param cert
	 * @return job id
	 * @throws JobSubmissionException
	 */
	public static int submitJob(String jdl, AliEnPrincipal user,
			X509Certificate[] cert) throws JobSubmissionException {

		try {
			if (JobSigner.verifyJob(cert, user, jdl)) {

				Job j = new Job();
				jobcounter++;
				System.out.println("Assigning jobID: " + jobcounter);
				j.queueId = jobcounter;

				j.status = "WAITING";
				j.userCertificate = cert[0];

				j.jdl = JobSigner.signJob(JAKeyStore.hostCert, "Host.cert",
						JAKeyStore.pass, user.getName(), jdl);

				j.site = "";
				j.started = 0;

				queue.put(jobcounter, j);

				System.out.println("We put the job in the QUEUE: " + j.jdl);

				return jobcounter;
			}
		} catch (InvalidKeyException e) {
			e.printStackTrace();
			System.out.println("InvalidKeyException");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.out.println("NoSuchAlgorithmException");
		} catch (SignatureException e) {
			e.printStackTrace();
			System.out.println("SignatureException");
		} catch (KeyStoreException e) {
			e.printStackTrace();
			System.out.println("KeyStoreException");
		}
		System.out.println("Job dismissed");

		return -1;

	}

	/**
	 * 
	 * @param jobID 
	 * @param jobnumber
	 * @param status
	 */
	public static void setJobStatus(int jobID, String status) {
		queue.get(jobID).status = status;
		System.out.println("Setting job [" + jobID + "] to status <"
				+ status + ">");
	}

	/**
	 * @param jobID
	 */
	public static String getJobStatus(int jobID) {
		if (jobID != 0 && queue.containsKey(jobID))
			if (queue.get(jobID) != null)
				return queue.get(jobID).status;
		return null;
	}

}
