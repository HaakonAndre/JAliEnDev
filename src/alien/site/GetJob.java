package alien.site;

import alien.broker.FakeQueueUtils;
import alien.taskQueue.Job;
import alien.ui.Request;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class GetJob extends Request {



	/**
	 * 
	 */
	private static final long serialVersionUID = -3575992501982425989L;
	private Job job;
	
	/**
	 */
	public GetJob(){
	}
	
	@Override
	public void run() {
		this.job = FakeQueueUtils.getJob();
	}
	
	/**
	 * @return a JDL
	 */
	public Job getJob(){
		return this.job;
	}
	
	@Override
	public String toString() {
		return "Asked for JDL :  reply is: "+this.job;
	}
}
