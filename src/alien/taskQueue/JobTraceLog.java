package alien.taskQueue;

import java.io.IOException;

import lazyj.Utils;

/**
 * @author steffen
 * @since Mar 3, 2011
 */
public class JobTraceLog {
	
	final private static String jobTraceLogURLPrefix = "http://aliendb8.cern.ch/joblog/";
	
	private String trace = "";
	
	/**
	 * @param id job ID
	 */
	JobTraceLog(final int id){
		final String queueId = String.valueOf(id);
		retrieve(jobTraceLogURLPrefix + queueId.substring(0,4) + "/" + queueId + ".log" );
	}
	
	private void retrieve(final String url ){
		try{
			trace = Utils.download(url, null);
		}
		catch (IOException ioe){
			// ignore
		}
	}
	  
    /**
     * @return the trace log
     */
    public String getTraceLog(){
    	return trace;
    }
}
