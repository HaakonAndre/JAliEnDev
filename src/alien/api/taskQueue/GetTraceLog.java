package alien.api.taskQueue;

import java.io.IOException;

import lazyj.Utils;
import alien.api.Request;

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
	 * @param queueId 
	 */
	public GetTraceLog(final int queueId){
		this.queueId = queueId;
	}
	
	@Override
	public void run() {
		final String queueId = String.valueOf(this.queueId);
		try{
			System.out.println("Trying to get job trace: " + jobTraceLogURLPrefix + queueId.substring(0,4) + "/" + queueId + ".log");
			trace = Utils.download(jobTraceLogURLPrefix + queueId.substring(0,4) + "/" + queueId + ".log" , null);
		}
		catch (IOException ioe){
			// ignore
		}
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
