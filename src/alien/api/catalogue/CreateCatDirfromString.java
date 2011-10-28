package alien.api.catalogue;

import alien.api.Request;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.user.AliEnPrincipal;

/**
 * 
 * @author ron
 * @since Jun 03, 2011
 */
public class CreateCatDirfromString extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8266256263179660234L;
	private final String path;
	private boolean createNonExistentParents = false;

	private LFN lfn;

	/**
	 * @param user
	 * @param role 
	 * @param path
	 * @param createNonExistentParents 
	 */
	public CreateCatDirfromString(final AliEnPrincipal user, final String role, final String path,boolean createNonExistentParents) {
		setRequestUser(user);
		setRoleRequest(role);
		this.path = path;
		this.createNonExistentParents = createNonExistentParents;
	}

	public void run() {
		// if(createNonExistentParents)
		this.lfn = FileSystemUtils.createCatalogueDirectory(getEffectiveRequester(), path,createNonExistentParents);
		// else 
		// 	this.lfn = FileSystemUtils.createCatalogueDirectory(user, path);

	}

	/**
	 * @return the created LFN of the directory
	 */
	public LFN getDir() {
		return this.lfn;
	}


	public String toString() {
		return "Asked for : " + this.path + ", reply is:\n" + this.lfn;
	}
}
