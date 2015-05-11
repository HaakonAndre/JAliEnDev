package alien.api.catalogue;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

public class TouchLFNfromString extends Request {

	private static final long serialVersionUID = -2792425667105358669L;
	private final String path;
	private boolean createNonExistentParents = false;
	
	private boolean success;

	private LFN lfn;
	
	/**
	 * @param user
	 * @param role 
	 * @param path
	 * @param createNonExistentParents 
	 */
	public TouchLFNfromString(final AliEnPrincipal user, final String role, final String path) {
		setRequestUser(user);
		setRoleRequest(role);
		this.path = path;
		//this.createNonExistentParents = createNonExistentParents;
	}
	
	@Override
	public void run() {
		// if(createNonExistentParents)
		this.lfn = LFNUtils.getLFN(path, true); 
		this.success = LFNUtils.touchLFN(getEffectiveRequester(), this.lfn);
		// else 
		// 	this.lfn = FileSystemUtils.createCatalogueDirectory(user, path);

	}

	/**
	 * @return the created LFN of the directory
	 */
	public LFN getLFN() {
		return this.lfn;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + ", reply is:\n" + this.lfn;
	}
}
