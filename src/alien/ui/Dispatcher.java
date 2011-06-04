package alien.ui;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

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
			System.out.println("Running centrally: " + r.toString());
			r.run();
			return r;
		}

		System.out.println("Running remote: " + r.toString());
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
		
		System.out.println("We received: " + response.getLFN().getFileName()  );
		
		long lStart = System.currentTimeMillis();
		
		for (int i=0; i<100; i++){
			request = new LFNfromString("/alice/cern.ch/user/g/grigoras/myNewFile", false);
			
			response = (LFNfromString) execute(request);
		
			//System.err.println(response);
		}
		
		System.err.println("Lasted : "+(System.currentTimeMillis() - lStart)+", "+SimpleClient.lSerialization);
		
		// dry run
		
		final long lStartDry = System.currentTimeMillis();
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		
		for (int i=0; i<100; i++){
			request = new LFNfromString("/alice/cern.ch/user/g/grigoras/myNewFile", false);
			
			oos.writeObject(request);
			oos.flush();
			baos.flush();
		}
		
		oos.close();
		
		System.err.println("Dry run took "+(System.currentTimeMillis() - lStartDry));
	}
	
}
