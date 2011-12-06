package alien.api.catalogue;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 * 
 * @author ron
 * @since Nov 21, 2011
 */
public class MoveLFNfromString extends Request {


	/**
	 * 
	 */
	private static final long serialVersionUID = 5206724705550117952L;

	private final String path;
	
	private final String newpath;
	
	private LFN newLFN = null;

	/**
	 * @param user 
	 * @param role 
	 * @param path
	 * @param newpath 
	 */
	public MoveLFNfromString(final AliEnPrincipal user, final String role, final String path, final String newpath) {
		setRequestUser(user);
		setRoleRequest(role);
		this.path = path;
		this.newpath = newpath;
	}

	@Override
	public void run() {
		LFN lfn = LFNUtils.getLFN(path);
		
		if(lfn!=null)
			newLFN = LFNUtils.mvLFN(getEffectiveRequester(), lfn, newpath);		

	}

	/**
	 * @return the status of the LFN's removal
	 */
	public LFN newLFN() {
		return this.newLFN;
	}


	@Override
	public String toString() {
		return "Asked to mv : " + this.path + " to " + this.newpath + ", reply is:\n" + this.newLFN;
	}
}
