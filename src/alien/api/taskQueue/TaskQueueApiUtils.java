package alien.api.taskQueue;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.shell.commands.JAliEnCOMMander;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import alien.quotas.Quota;
import alien.quotas.FileQuota;


/**
 * Get the JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class TaskQueueApiUtils {

	private final JAliEnCOMMander commander;

	/**
	 * @param commander
	 */
	public TaskQueueApiUtils(JAliEnCOMMander commander) {
		this.commander = commander;
	}

	/**
	 * @return the uptime / w statistics
	 */
	public static Map<String, GetUptime.UserStats> getUptime() {
		try {
			final GetUptime uptime = Dispatcher.execute(new GetUptime());

			return uptime.getStats();
		}
		catch (ServerException e) {
			System.out.println("Could not get an uptime stats: " + e.getMessage());
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * @param states
	 * @param users
	 * @param sites
	 * @param nodes
	 * @param mjobs
	 * @param jobid
	 * @param orderByKey
	 * @param limit
	 * @return a PS listing
	 */
	public List<Job> getPS(final Collection<JobStatus> states, final Collection<String> users, final Collection<String> sites, final Collection<String> nodes, final Collection<Integer> mjobs,
			final Collection<Integer> jobid, final String orderByKey, final int limit) {

		try {
			GetPS ps = Dispatcher.execute(new GetPS(commander.getUser(), commander.getRole(), states, users, sites, nodes, mjobs, jobid, orderByKey, limit));

			return ps.returnPS();
		}
		catch (ServerException e) {
			System.out.println("Could not get a PS listing: " + e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * @param status
	 * @param id
	 * @param jobId
	 * @param site
	 * @param bPrintId
	 * @param bPrintSite
	 * @param bMerge
	 * @param bKill
	 * @param bResubmit
	 * @param bExpunge
	 * @return a PS listing
	 */
	public List<Job> getMasterJobStatus(final int jobId, final Set<JobStatus> status, final List<Integer> id, final List<String> site, final boolean bPrintId, final boolean bPrintSite,
			final boolean bMerge, final boolean bKill, final boolean bResubmit, final boolean bExpunge) {

		try {
			GetMasterjob mj = Dispatcher.execute(new GetMasterjob(commander.getUser(), commander.getRole(), jobId, status, id, site, bPrintId, bPrintSite, bMerge, bKill, bResubmit,bExpunge));

			// return mj.masterJobStatus();
			return mj.subJobStatus();

		}
		catch (ServerException e) {
			System.out.println("Could get a PS listing: ");
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * @param queueId
	 * @return a JDL as String
	 */
	public String getTraceLog(final int queueId) {

		try {
			GetTraceLog trace = Dispatcher.execute(new GetTraceLog(commander.getUser(), commander.getRole(), queueId));

			return trace.getTraceLog();
		}
		catch (ServerException e) {
			System.out.println("Could get not a TraceLog: ");
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * @param queueId
	 * @return a JDL as String
	 */
	public String getJDL(final int queueId) {

		try {
			GetJDL jdl = Dispatcher.execute(new GetJDL(commander.getUser(), commander.getRole(), queueId));

			return jdl.getJDL();
		}
		catch (ServerException e) {
			System.out.println("Could get not a JDL: "+e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * @param queueId
	 * @return a Job
	 */
	public Job getJob(final int queueId) {

		try {
			GetJob job = Dispatcher.execute(new GetJob(commander.getUser(), commander.getRole(), queueId));

			return job.getJob();
		}
		catch (ServerException e) {
			System.out.println("Could get not the Job: "+e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * @param queueIds
	 * @return a Job
	 */
	public List<Job> getJobs(final List<Integer> queueIds) {

		try {
			GetJobs job = Dispatcher.execute(new GetJobs(commander.getUser(), commander.getRole(), queueIds));

			return job.getJobs();
		}
		catch (ServerException e) {
			System.out.println("Could get not the Jobs: "+e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * Set a job's status
	 * 
	 * @param jobnumber
	 * @param status
	 */
	public static void setJobStatus(final int jobnumber, final JobStatus status) {
		try {
			Dispatcher.execute(new SetJobStatus(jobnumber, status));

		}
		catch (ServerException e) {
			System.out.println("Could get not a Job's status: "+e.getMessage());
			e.getCause().printStackTrace();
		}
	}

	/**
	 * Submit a job
	 * 
	 * @param jdl
	 * @return queueId
	 * @throws ServerException 
	 */
	public int submitJob(final JDL jdl) throws ServerException {

			//final JDL signedJDL = JobSigner.signJob(JAKeyStore.clientCert, "User.cert", JAKeyStore.pass, commander.getUser().getName(), ojdl);
			
			final SubmitJob j = new SubmitJob(commander.getUser(), commander.getRole(), jdl);
			
			final SubmitJob response = Dispatcher.execute(j);
			
			return response.getJobID();

	}

	/**
	 * Kill a job
	 * 
	 * @param queueId
	 * 
	 * @return status of the kill
	 */
	public boolean killJob(int queueId) {

		try {
			KillJob j = new KillJob(commander.getUser(), commander.getRole(), queueId);

			Dispatcher.execute(j);
			return j.wasKilled();

		}
		catch (Exception e) {
			System.out.println("Could not kill the job  with id: [" + queueId + "]");
			e.printStackTrace();
		}
		return false;
	}
	
	public Quota getJobsQuota(){
		try{
			GetQuota gq = new GetQuota(commander.getUser());
			GetQuota gqres = Dispatcher.execute(gq);
			return gqres.getQuota();
		}
		catch( Exception e ){
			System.out.println( "Exception in GetQuota: " + e.getMessage() );
		}
		return null;
	}

	
	public FileQuota getFileQuota(){
		try{
			GetFileQuota gq = new GetFileQuota(commander.getUser());
			GetFileQuota gqres = Dispatcher.execute(gq);
			
			return gqres.getFileQuota();
		}
		catch( Exception e ){
			System.out.println( "Exception in GetFileQuota: " + e.getMessage() );
		}
		return null;
	}
}
