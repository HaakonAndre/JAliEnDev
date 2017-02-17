package alien.site.batchqueue;

import java.util.HashMap;
import java.util.logging.Logger;

/**
 * Base interface for batch queues
 *
 * @author mmmartin
 */
public abstract class BatchQueue {
	public static transient Logger logger = null;
	public static HashMap<String, Object> env = null;

	public abstract void submit();

	public abstract int getNumberActive();

	public abstract int getNumberQueued();

	public abstract int kill();
}
