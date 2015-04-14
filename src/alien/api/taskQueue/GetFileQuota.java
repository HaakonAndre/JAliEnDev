package alien.api.taskQueue;

import alien.api.Request;
import alien.quotas.FileQuota;
import alien.quotas.QuotaUtilities;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

public class GetFileQuota extends Request {
	private String username;
	private FileQuota q;
	
	/**
	 * @param user 
	 * @param role 
	 * @param queueId
	 */
	public GetFileQuota(final AliEnPrincipal user) {
		this.username = user.getName();
	}
	
	@Override
	public void run() {
		this.q = QuotaUtilities.getFileQuota( this.username );		
	}
	
	public FileQuota getFileQuota(){
		return this.q;
	}
}
