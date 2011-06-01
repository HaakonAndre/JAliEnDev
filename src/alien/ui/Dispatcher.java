package alien.ui;

import java.io.IOException;

import alien.config.ConfigUtils;
import alien.ui.api.LFNfromString;

/**
 * @author costing
 * @since 2011-03-04
 */
public class Dispatcher {

	
	
	/**
	 * @param r request to execute
	 * @return the processed request
	 * @throws IOException exception thrown by the processing
	 */
	public static Request execute(final Request r) throws IOException{
		if (ConfigUtils.isCentralService()){
			r.run();
			return r;
		}

		return SimpleClient.dispatchRequest(r);
	}

	/**
	 * Debug method
	 * 
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		LFNfromString request = new LFNfromString("/alice/cern.ch/user/g/grigoras/myNewFile", false);
		
		LFNfromString response = (LFNfromString) execute(request);
		
		long lStart = System.currentTimeMillis();
		
		for (int i=0; i<100; i++){
			request = new LFNfromString("/alice/cern.ch/user/g/grigoras/myNewFile", false);
			
			response = (LFNfromString) execute(request);
		
			//System.err.println(response);
		}
		
		System.err.println("Lasted : "+(System.currentTimeMillis() - lStart)+", "+SimpleClient.lSerialization);
	}
	
}
