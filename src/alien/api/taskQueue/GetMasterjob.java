package alien.api.taskQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import alien.api.Request;
import alien.config.ConfigUtils;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class GetMasterjob extends Request {


	/**
	 * 
	 */
	private static final long serialVersionUID = -2299330738741897654L;


	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(TaskQueueUtils.class.getCanonicalName());
	

	/**
	 * 
	 */
	private List<Job>   subJobs = null;
	
	private  Job   masterJob = null;


	private final int jobId;
	
	private final Set<JobStatus> status;
	
	private final List<Integer> id;
	
	private final List<String> site;
	
	private final boolean bPrintId;

	private final boolean bPrintSite;

	private final boolean bMerge;

	private final boolean bKill;

	private final boolean bResubmit;

	private final boolean bExpunge;
	
	/**
	 * @param user 
	 * @param role 
	 * @param jobId 
	 * @param status 
	 * @param id 
	 * @param site 
	 * @param bPrintId 
	 * @param bPrintSite 
	 * @param bMerge 
	 * @param bKill 
	 * @param bResubmit 
	 * @param bExpunge 
	 */
	public GetMasterjob(final AliEnPrincipal user, final String role, final int jobId,  final Set<JobStatus> status, final List<Integer> id, final List<String> site,
			final boolean bPrintId, final boolean bPrintSite, final boolean bMerge, final boolean bKill, final boolean bResubmit, final boolean bExpunge){
		setRequestUser(user);
		setRoleRequest(role);
		this.jobId = jobId;
		this.status = status;
		this.id = id;
		this.site = site;
		this.bPrintId = bPrintId;
		this.bPrintSite = bPrintSite;
		this.bMerge = bMerge;
		this.bKill = bKill;
		this.bResubmit = bResubmit;
		this.bExpunge = bExpunge;
	}
	
	
	@Override
	public void run() {
		
		System.out.println("running with GetMasterJob");


		subJobs = TaskQueueUtils.getMasterJobStat(jobId, status, id, site, bPrintId, bPrintSite, bMerge, bKill, 
				bResubmit, bExpunge,10000);
		
		System.out.println("got subjosb, in GetMasterJob");

		
		masterJob = TaskQueueUtils.getJob(jobId);
		
		System.out.println("done with GetMasterJob");
		
	}
	

	/**
	 * 
	 * @return the masterjob
	 */
	public List<Job> subJobStatus(){
		return  this.subJobs;
	}
	
	/**
	 * 
	 * @return the masterjob
	 */
	public HashMap<Job,List<Job>> masterJobStatus(){

		HashMap<Job,List<Job>> masterjobstatus = new HashMap<Job,List<Job>>();
		
		masterjobstatus.put(this.masterJob, this.subJobs);		
		
		return masterjobstatus;
	}
	
	
	@Override
	public String toString() {
		return "Asked for Masterjob status :  reply is: "+this.masterJob;
	}
}
