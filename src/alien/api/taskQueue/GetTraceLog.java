package alien.api.taskQueue;

import java.io.IOException;

import lazyj.Utils;
import alien.api.Request;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Get a TraceLog object
 * 
 * @author ron
 * @since Oct 26, 2011
 */
public class GetTraceLog extends Request {


	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5022083696413315512L;

	final private static String jobTraceLogURLPrefix = "http://aliendb8.cern.ch/joblog/";
	
	private String trace = "";
	
	private final int queueId;
	
	private void retrieve(final String url ){
		try{
			trace = Utils.download(url, null);
		}
		catch (IOException ioe){
			// ignore
		}
	}
	
	/**
	 * @param user 
	 * @param role 
	 * @param queueId 
	 */
	public GetTraceLog(final AliEnPrincipal user, final String role, final int queueId){
		setRequestUser(user);
		setRoleRequest(role);
		this.queueId = queueId;
	}
	
	@Override
	public void run() {

		trace = TaskQueueUtils.getJobTraceLog(queueId);
	}
	
	/**
	 * @return a JDL
	 */
	public String getTraceLog(){
		return this.trace;
	}
	
	@Override
	public String toString() {
		return "Asked for TraceLog :  reply is: "+this.trace;
	}
}
