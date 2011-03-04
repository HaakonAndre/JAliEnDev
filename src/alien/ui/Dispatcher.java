package alien.ui;

import alien.config.ConfigUtils;

/**
 * @author costing
 * @since 2011-03-04
 */
public class Dispatcher {

	/**
	 * @param r request to execute
	 */
	public static void execute(final Request r){
		if (ConfigUtils.isCentralService()){
			r.run();
		}
		else{
			throw new IllegalAccessError("Remote calling not implemented yet");
		}
	}
	
}
