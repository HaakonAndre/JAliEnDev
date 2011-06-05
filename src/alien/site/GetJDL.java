package alien.site;

import alien.broker.FakeQueueUtils;
import alien.taskQueue.JDL;
import alien.ui.Request;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class GetJDL extends Request {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	private JDL jdl;
	
	/**
	 */
	public GetJDL(){
	}
	
	@Override
	public void run() {
		this.jdl = FakeQueueUtils.getJDL();
	}
	
	/**
	 * @return a JDL
	 */
	public JDL getJDL(){
		return this.jdl;
	}
	
	@Override
	public String toString() {
		return "Asked for JDL :  reply is: "+this.jdl;
	}
}
