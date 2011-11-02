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
