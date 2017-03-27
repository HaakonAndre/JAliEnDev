package alien.site.batchqueue;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.logging.Logger;

import alien.log.LogUtils;

/**
 * @author mmmartin
 */
public class FORK extends BatchQueue {

	/**
	 * @param envi
	 *            execution environment
	 * @param logr
	 *            logger
	 */
	public FORK(HashMap<String, Object> envi, Logger logr) {
		env = envi;
		logger = logr;
		logger = LogUtils.redirectToCustomHandler(logger, ((String) env.get("logdir")) + "JAliEn." + (new Timestamp(System.currentTimeMillis()).getTime() + ".out"));
	}

	@Override
	public void submit(final String script) {
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
