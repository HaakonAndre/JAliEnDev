package alien.api.catalogue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 * Get the LFN object for this path
 *
 * @author costing
 * @since 2011-03-04
 */
public class LFNfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -1720547988105993480L;

	private final Collection<String> path;
	private final boolean ignoreFolders;
	private final boolean evenIfDoesntExist;

	private List<LFN> lfns = null;

	/**
	 * @param user
	 * @param role
	 * @param paths
	 * @param ignoreFolders
	 * @param evenIfDoesntExist
	 *
	 */
	public LFNfromString(final AliEnPrincipal user, final String role, final boolean ignoreFolders, final boolean evenIfDoesntExist, final Collection<String> paths) {
		setRequestUser(user);
		setRoleRequest(role);
		this.path = paths;
		this.ignoreFolders = ignoreFolders;
		this.evenIfDoesntExist = evenIfDoesntExist;
	}

	@Override
	public void run() {
		if (evenIfDoesntExist)
			for (final String s : path) {
				final LFN l = LFNUtils.getLFN(s, evenIfDoesntExist);

				if (l != null && !(l.isDirectory() && ignoreFolders)) {
					if (this.lfns == null)
						this.lfns = new ArrayList<>();

					this.lfns.add(l);
				}
			}
		else
			this.lfns = LFNUtils.getLFNs(ignoreFolders, path);
	}

	/**
	 * @return the requested LFN
	 */
	public List<LFN> getLFNs() {
		return this.lfns;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + " (" + this.ignoreFolders + "), reply is: " + this.lfns;
	}
}
