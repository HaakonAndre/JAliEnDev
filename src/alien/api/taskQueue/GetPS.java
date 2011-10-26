package alien.api.taskQueue;

import java.util.List;

import alien.api.Request;
import alien.taskQueue.Job;
import alien.taskQueue.TaskQueueUtils;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class GetPS extends Request {



	/**
	 * 
	 */
	private static final long serialVersionUID = -1486633687303580187L;

	/**
	 * 
	 */
	private List<Job> jobs;
	
	private final boolean running;
	
	/**
	 * @param loadJDL 
	 * @param running 
	 */
	public GetPS(final boolean running){
		this.running = running;
	}
	
	@Override
	public void run() {
		this.jobs = TaskQueueUtils.getPS(running);
	}
	
	/**
	 * @return a JDL
	 */
	public List<Job> returnPS(){
		return this.jobs;
	}
	
	@Override
	public String toString() {
		return "Asked for PS :  reply is: "+this.jobs;
	}
}
