package alien.site;

import java.io.IOException;

import alien.taskQueue.Job;
import alien.ui.Dispatcher;


/**
 * Get the JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class JobAgentUtils {
	
	/**
	 * @return a Job
	 */
	public static Job getJob() {

		try {
			GetJob job = (GetJob) Dispatcher.execute(new GetJob(),true);

			return job.getJob();
		} catch (IOException e) {
			System.out.println("Could not a JDL: ");
			e.printStackTrace();
		}
		return null;

	}
	
	
	/**
	 * Set a job's status
	 * @param jobnumber 
	 * @param status 
	 */
	public static void setJobStatus(int jobnumber, String status) {

		try {
			Dispatcher.execute(new SetJobStatus(jobnumber,status),true);

		} catch (IOException e) {
			System.out.println("Could not a JDL: ");
			e.printStackTrace();
		}
	}

}
