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
	 * @param conf
	 * @param logr
	 *            logger
	 */
	public FORK(HashMap<String, Object> conf, Logger logr) {
		this.config = conf;
		logger = logr;
		logger = LogUtils.redirectToCustomHandler(logger, ((String) config.get("host_logdir")) + "JAliEn." + (new Timestamp(System.currentTimeMillis()).getTime() + ".out"));

		logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") + ", site is " + config.get("site_accountName"));
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
