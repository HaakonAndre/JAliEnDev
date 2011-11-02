/**
 * 
 */
package alien.taskQueue;

import java.util.HashMap;
import java.util.Map;

/**
 * @author costing
 * @since Nov 2, 2011
 */
public class JobStatusFactory {

	private static final Map<String, JobStatus> stringToStatus = new HashMap<String, JobStatus>();
	
	
	// don't delete this, on the user site, I need all of them initialized!
	static{
		stringToStatus.put("INSERTING",JobStatus.INSERTING);
		stringToStatus.put("SPLITTING",JobStatus.SPLITTING);
		stringToStatus.put("SPLIT",JobStatus.SPLIT);
		stringToStatus.put("TO_STAGE",JobStatus.TO_STAGE);
		stringToStatus.put("A_STAGED",JobStatus.A_STAGED);
		stringToStatus.put("STAGING",JobStatus.STAGING);
		stringToStatus.put("WAITING",JobStatus.WAITING);
		stringToStatus.put("OVER_WAITING",JobStatus.OVER_WAITING);
		stringToStatus.put("ASSIGNED",JobStatus.ASSIGNED);
		stringToStatus.put("QUEUED",JobStatus.QUEUED);
		stringToStatus.put("STARTED",JobStatus.STARTED);
		stringToStatus.put("IDLE",JobStatus.IDLE);
		stringToStatus.put("INTERACTIV",JobStatus.INTERACTIV);
		stringToStatus.put("RUNNING",JobStatus.RUNNING);
		stringToStatus.put("SAVING",JobStatus.SAVING);
		stringToStatus.put("SAVED",JobStatus.SAVED);
		stringToStatus.put("DONE",JobStatus.DONE);
		stringToStatus.put("SAVED_WARN",JobStatus.SAVED_WARN);
		stringToStatus.put("DONE_WARN",JobStatus.DONE_WARN);
		stringToStatus.put("ERROR_A",JobStatus.ERROR_A);
		stringToStatus.put("ERROR_I",JobStatus.ERROR_I);
		stringToStatus.put("ERROR_E",JobStatus.ERROR_E);
		stringToStatus.put("ERROR_IB",JobStatus.ERROR_IB);
		stringToStatus.put("ERROR_M",JobStatus.ERROR_M);
		stringToStatus.put("ERROR_RE",JobStatus.ERROR_RE);
		stringToStatus.put("ERROR_S",JobStatus.ERROR_S);
		stringToStatus.put("ERROR_SV",JobStatus.ERROR_SV);
		stringToStatus.put("ERROR_V",JobStatus.ERROR_V);
		stringToStatus.put("ERROR_VN",JobStatus.ERROR_VN);
		stringToStatus.put("ERROR_VT",JobStatus.ERROR_VT);
		stringToStatus.put("ERROR_SPLT",JobStatus.ERROR_SPLT);
		stringToStatus.put("EXPIRED",JobStatus.EXPIRED);
		stringToStatus.put("FAILED",JobStatus.FAILED);
		stringToStatus.put("KILLED",JobStatus.KILLED);
		stringToStatus.put("FORCEMERGE",JobStatus.FORCEMERGE);
		stringToStatus.put("MERGING",JobStatus.MERGING);
		stringToStatus.put("ZOMBIE",JobStatus.ZOMBIE);
	}
	
	/**
	 * Register a new job status
	 * 
	 * @param status
	 */
	static void addStatus(final JobStatus status){
		stringToStatus.put(status.name(), status);
	}
	
	/**
	 * @param status
	 * @return the status, as object
	 */
	public static final JobStatus getByStatusName(final String status){
		return stringToStatus.get(status);
	}
	
	
}
