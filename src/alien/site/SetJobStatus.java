package alien.site;

import alien.broker.FakeQueueUtils;
import alien.ui.Request;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class SetJobStatus extends Request {

	
	


	/**
	 * 
	 */
	private static final long serialVersionUID = -6330031807464568209L;
	private int jobnumber = 0;
	private String status = null;
	
	/**
	 * @param jobnumber 
	 * @param status 
	 */
	public SetJobStatus(int jobnumber, String status){
		this.jobnumber = jobnumber;
		this.status = status;
	}
	
	@Override
	public void run() {
		FakeQueueUtils.setJobStatus(this.jobnumber, this.status);
	}

	
	@Override
	public String toString() {
		return "Asked to set job ["+this.jobnumber+"] to status: "+this.status;
	}
}
