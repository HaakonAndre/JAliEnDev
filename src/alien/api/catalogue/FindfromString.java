package alien.api.catalogue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 *
 * @author ron
 * @since Jun 06, 2011
 */
public class FindfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -5938936122293608584L;
	private final String path;
	private final String pattern;
	private final int flags;
	private Collection<LFN> lfns;

	/**
	 * @param user
	 * @param path
	 * @param pattern
	 * @param flags
	 */
	public FindfromString(final AliEnPrincipal user, final String path, final String pattern, final int flags) {
		setRequestUser(user);
		this.path = path;
		this.pattern = pattern;
		this.flags = flags;
	}

	@Override
	public void run() {
		lfns = LFNUtils.find(path, pattern, flags);
	}

	/**
	 * @return the found LFNs
	 */
	public Collection<LFN> getLFNs() {
		return this.lfns;
	}

	@Override
	public String toString() {
		return "Asked for : path (" + this.path + "), pattern (" + this.pattern + "), flags (" + this.flags + ") reply is:\n" + this.lfns;
	}

	/**
	 * Made by sraje (Shikhar Raje, IIIT Hyderabad) // *
	 *
	 * @return the list of file names (one level down only) that matched the
	 *         find
	 */
	public List<String> getFileNames() {
		if (lfns == null)
			return null;

		final List<String> ret = new ArrayList<>(lfns.size());

		for (final LFN l : lfns)
			ret.add(l.getFileName());

		return ret;
	}
}
