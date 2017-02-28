package alien.site.batchqueue;

import java.util.HashMap;
import java.util.logging.Logger;

/**
 * @author mmmartin
 */
public class FORK extends BatchQueue {

	/**
	 * @param envi execution environment
	 * @param logr logger
	 */
	public FORK(HashMap<String, Object> envi, Logger logr) {
		env = envi;
		logger = logr;
	}

	@Override
	public void submit() {
		logger.info("Submit FORK");
	}

	@Override
	public int getNumberActive() {
		return 0;
	}

	@Override
	public int getNumberQueued() {
		return 0;
	}

	@Override
	public int kill() {
		return 0;
	}

}
