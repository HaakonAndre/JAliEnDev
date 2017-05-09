package alien.site.batchqueue;

import java.util.HashMap;
import java.util.logging.Logger;

/**
 * Base interface for batch queues
 *
 * @author mmmartin
 */
public abstract class BatchQueue {
	protected Logger logger = null;
	protected HashMap<String, Object> env = null;

	/**
	 * Submit a new job agent to the queue
	 * @param script 
	 */
	public abstract void submit(final String script);

	/**
	 * @return number of currently active jobs
	 */
	public abstract int getNumberActive();

	/**
	 * @return number of queued jobs
	 */
	public abstract int getNumberQueued();

	/**
	 * @return how many jobs were killed
	 */
	public abstract int kill();
}
