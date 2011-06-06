package alien;

import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.SimpleCatalogueApiService;
import alien.api.SimpleTaskQueueApiService;
import alien.config.ConfigUtils;


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
			SimpleCatalogueApiService catalogueAPIService = new SimpleCatalogueApiService();
			catalogueAPIService.start();
			SimpleTaskQueueApiService taskqueueAPIService = new SimpleTaskQueueApiService();
			taskqueueAPIService.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
