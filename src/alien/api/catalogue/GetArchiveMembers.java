package alien.api.catalogue;

import java.util.List;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 * @author yuw
 * @since 2018-05-29
 * 
 */
public class GetArchiveMembers extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6542969312522391743L;

	private LFN archive;
	private String sArchive;

	private List<LFN> members = null;

	/**
	 * Resolve the archive member and the real LFN of it
	 * 
	 * @param user
	 *            user who makes the request
	 * @param sArchive
	 *            name of an archive to find its members
	 */
	public GetArchiveMembers(final AliEnPrincipal user, final String sArchive) {
		setRequestUser(user);
		this.sArchive = sArchive;
		this.archive = null;
	}

	@Override
	public void run() {
		if (this.archive == null)
			this.archive = LFNUtils.getLFN(sArchive);

		if (this.archive == null)
			return;

		this.members = LFNUtils.getArchiveMembers(archive);
	}

	/**
	 * @return the archive members that were requested (or resolved from a string)
	 */
	public List<LFN> getArchiveMembers() {
		return this.members;
	}
}
