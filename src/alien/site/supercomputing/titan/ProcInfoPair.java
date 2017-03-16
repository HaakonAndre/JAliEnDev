package alien.site.supercomputing.titan;

/**
 * @author psvirin
 *
 */
public class ProcInfoPair {
	/**
	 * Job ID
	 */
	public final long queue_id;

	/**
	 * Proc line to add to the trace
	 */
	public final String procinfo;

	/**
	 * @param queue_id
	 * @param procinfo
	 */
	public ProcInfoPair(final String queue_id, final String procinfo) {
		this.queue_id = Long.parseLong(queue_id);
		this.procinfo = procinfo;
	}
}
