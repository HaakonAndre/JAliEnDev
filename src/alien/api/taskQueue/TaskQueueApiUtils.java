package alien.api.taskQueue;

import java.io.IOException;

import alien.api.Dispatcher;
import alien.taskQueue.Job;


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
