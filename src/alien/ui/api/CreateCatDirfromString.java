package alien.ui.api;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.ui.Request;
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
	private AliEnPrincipal user = null;
	private boolean createNonExistentParents = false;

	private LFN lfn;

	/**
	 * @param user
	 * @param path
	 * @param createNonExistentParents 
	 */
	public CreateCatDirfromString(AliEnPrincipal user, final String path,boolean createNonExistentParents) {
		this.user = user;
		this.path = path;
		this.createNonExistentParents = createNonExistentParents;
	}

	@Override
	public void run() {
		// if(createNonExistentParents)
		this.lfn = FileSystemUtils.createCatalogueDirectory(user, path,createNonExistentParents);
		// else 
		// 	this.lfn = FileSystemUtils.createCatalogueDirectory(user, path);

	}

	/**
	 * @return the created LFN of the directory
	 */
	public LFN getDir() {
		return this.lfn;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + ", reply is:\n" + this.lfn;
	}
}
