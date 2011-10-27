package alien.api.catalogue;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 * 
 * @author ron
 * @since Oct 27, 2011
 */
public class RemoveLFNfromString extends Request {


	/**
	 * 
	 */
	private static final long serialVersionUID = 8507879864667855615L;
	private final String path;
	private AliEnPrincipal user = null;

	
	private boolean wasRemoved = false;

	/**
	 * @param user 
	 * @param path
	 */
	public RemoveLFNfromString(AliEnPrincipal user, final String path) {
		this.user = user;
		this.path = path;
	}

	public void run() {
		LFN lfn = LFNUtils.getLFN(path);
		if(lfn!=null)
			wasRemoved = LFNUtils.rmLFN(user, lfn);		

	}

	/**
	 * @return the status of the LFN's removal
	 */
	public boolean wasRemoved() {
		return this.wasRemoved;
	}


	public String toString() {
		return "Asked to remove : " + this.path + ", reply is:\n" + this.wasRemoved;
	}
}
