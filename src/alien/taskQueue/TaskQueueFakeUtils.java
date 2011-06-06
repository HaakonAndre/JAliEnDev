package alien.taskQueue;

import java.util.HashMap;

import lazyj.Utils;

/**
 * @author ron
 *  @since Jun 05, 2011
 */
public class TaskQueueFakeUtils {

	private static int jobcounter = (int) System.currentTimeMillis() % 10000000;
	
	private static HashMap<Integer,Job> queue = new HashMap<Integer,Job>();
		
	/**
	 * @return a job
	 */
	public static Job getJob(){
		
		Job j = fakeJob();
		setJobStatus(j.queueId, "ASSIGNED");
		return j;
	}
	
	/**
	 * @return fake job
	 */
	public static Job fakeJob(){
		Job j = new Job();
		jobcounter++;
		j.queueId = jobcounter;
		
		j.status = "WAITING";
		
		j.jdl = Utils.readFile("/tmp/myFirst.jdl");
		
		j.site = "";
		j.started = 0;
		queue.put(jobcounter,j);
		return j;
	}
	
	/**
	 * @param jobnumber
	 * @param status
	 */
	public static void setJobStatus(int jobnumber, String status){
		queue.get(jobnumber).status = status;
		System.out.println("Setting job [" + jobnumber + "] to status <"+ status + ">");
	}

}
