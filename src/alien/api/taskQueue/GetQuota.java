package alien.api.taskQueue;

import alien.api.Request;
import alien.quotas.Quota;
import alien.quotas.QuotaUtilities;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

public class GetQuota extends Request {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7852648478812622364L;
	private String username;
	private Quota q;
	
	/**
	 * @param user 
	 * @param role 
	 * @param queueId
	 */
	public GetQuota(final AliEnPrincipal user) {
		this.username = user.getName();
	}
	
	@Override
	public void run() {
		this.q = QuotaUtilities.getJobQuota( this.username );		
	}
	
	public Quota getQuota(){
		return this.q;
	}
}
