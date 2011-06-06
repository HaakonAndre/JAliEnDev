package alien;

import java.util.logging.Level;
import java.util.logging.Logger;

import alien.broker.SimpleJobBroker;
import alien.config.ConfigUtils;
import alien.ui.SimpleApi;


/**
 * @author ron
 * @since Jun 6, 2011
 */
public class CentralServices {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Testing.class
			.getCanonicalName());

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		logger.setLevel(Level.WARNING);

		try {
			SimpleApi api = new SimpleApi();
			api.start();
			SimpleJobBroker jb = new SimpleJobBroker();
			jb.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
