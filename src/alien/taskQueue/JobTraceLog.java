package alien.taskQueue;

import java.io.IOException;
import java.net.MalformedURLException;

import alien.protocols.HTTP;

public class JobTraceLog {
	
	final private String jobTraceLogURLPrefix = "http://aliendb8.cern.ch/joblog/";
	
	private String trace = "";
	
	JobTraceLog(int id){
		String queueId = String.valueOf(id);
		retrieve(jobTraceLogURLPrefix + queueId.substring(0,4) + "/" + queueId + ".log" );
	}
	
	private void retrieve(String url ){
		HTTP http = new HTTP();
		try {
			trace = http.retrieve(url);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	  
    public String getTraceLog(){
    	return trace;
    }
    
}
