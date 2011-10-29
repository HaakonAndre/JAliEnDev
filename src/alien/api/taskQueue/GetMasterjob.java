package alien.api.taskQueue;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.config.ConfigUtils;
import alien.taskQueue.Job;
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
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(TaskQueueUtils.class.getCanonicalName());
	

	/**
	 * 
	 */
	private static final long serialVersionUID = -453943843524253526L;

	/**
	 * 
	 */
	private List<Job> jobs = null;

	private final String jobId;
	
	private final String status;
	
	private final String id;
	
	private final String site;
	
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
	public GetMasterjob(final AliEnPrincipal user, final String role, final String jobId, final String status, final String id, final String site,
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
		
		if(bKill)
			System.out.println("sending: " + jobId );
	}
	
	
	@Override
	public void run() {
		
		logger.log(Level.INFO, "Running masterjob <IN>");
		if(jobId!=null)
			logger.log(Level.INFO, "jobId: " + jobId );
		if(status!=null)
			logger.log(Level.INFO, "status: " + status );
		if(id!=null)
			logger.log(Level.INFO, "id: " + id );
		if(site!=null)
			logger.log(Level.INFO, "site: " + site );
		if(bPrintId)
			logger.log(Level.INFO, "bPrintId");
		if(bPrintSite)
			logger.log(Level.INFO, "bPrintSite");
		if(bKill)
			logger.log(Level.INFO, "kill");
		if(bResubmit)
			logger.log(Level.INFO, "resubmit");
		if(bExpunge)
			logger.log(Level.INFO, "bExpunge" );
		if(bMerge)
			logger.log(Level.INFO, "bMerge");
		logger.log(Level.INFO, "Running masterjob <IN>");

		//this.jobs = 
	}
	
	/**
	 * @return a JDL
	 */
	public List<Job> returnMasterjob(){
		if(jobId!=null)
			System.out.println("jobId: " + jobId );
		if(status!=null)
			System.out.println("status: " + status );
		if(id!=null)
			System.out.println("id: " + id );
		if(site!=null)
			System.out.println("site: " + site );
		if(bPrintId)
			System.out.println("bPrintId");
		if(bPrintSite)
			System.out.println("bPrintSite");
		if(bKill)
			System.out.println("kill");
		if(bResubmit)
			System.out.println("resubmit");
		if(bExpunge)
			System.out.println("bExpunge" );
		if(bMerge)
			System.out.println("bMerge");
		return this.jobs;
	}
	
	@Override
	public String toString() {
		return "Asked for Masterjob :  reply is: "+this.jobs;
	}
}
