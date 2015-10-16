package alien.api.catalogue;

import java.util.Collection;
import java.util.HashMap;

import alien.api.Request;
import alien.catalogue.CatalogEntity;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * chown request
 */
public class ChownLFN extends Request {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4209526023185462132L;
	private final String path;
	private final String chown_user;
	private final String chown_group;
	private boolean success;
	private final boolean recursive;
	private HashMap<String, Boolean> results;

	/**
	 * @param user
	 * @param role
	 * @param fpath
	 * @param chuser
	 * @param chgroup
	 * @param recursive
	 */
	public ChownLFN(final AliEnPrincipal user, final String role, final String fpath, final String chuser, final String chgroup, final boolean recursive) {
		this.path = fpath;
		this.chown_user = chuser;
		this.chown_group = chgroup;
		this.recursive = recursive;
	}

	@Override
	public void run() {
		// if( !AliEnPrincipal.roleIsAdmin( getEffectiveRequester().getName() ) )
		// throw new SecurityException( "Only administrators can do it" );

		this.results = new HashMap<>();

		final CatalogEntity c = LFNUtils.getLFN(this.path);
		if (!AuthorizationChecker.isOwner(c, getEffectiveRequester()))
			this.success = false;
		else
			// throw new SecurityException("You do not own this file: " + c +
			// ", requester: " + getEffectiveRequester() );
			this.success = LFNUtils.chownLFN(this.path, this.chown_user, this.chown_group);
		results.put(this.path, Boolean.valueOf(this.success));

		if (!this.recursive || !this.success)
			return;

		final Collection<LFN> lfns = LFNUtils.find(this.path, "*", LFNUtils.FIND_INCLUDE_DIRS);
		for (final LFN l : lfns) {
			if (!AuthorizationChecker.isOwner(l, getEffectiveRequester()))
				this.success = false;
			else
				// throw new SecurityException("You do not own this file: " + l +
				// ", requester: " + getEffectiveRequester() );
				this.success = LFNUtils.chownLFN(l.getCanonicalName(), this.chown_user, this.chown_group);
			results.put(this.path, Boolean.valueOf(this.success));
		}
	}

	/**
	 * @return operation status
	 */
	public boolean getSuccess() {
		return this.success;
	}

	/**
	 * @return operation status per lfn
	 */
	public HashMap<String, Boolean> getResults() {
		return this.results;
	}

}
