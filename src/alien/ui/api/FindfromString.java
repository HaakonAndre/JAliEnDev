package alien.ui.api;

import java.util.List;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.ui.Request;

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
	private final String pattern ;
	private final int flags;
	private List<LFN>  lfns;

	/**
	 * @param path 
	 * @param pattern 
	 * @param flags 
	 */
	public FindfromString(final String path, final String pattern, final int flags) {
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
	public List<LFN> getLFNs() {
		return this.lfns;
	}

	@Override
	public String toString() {
		return "Asked for : path (" + this.path +"), pattern ("+this.pattern
				+ "), flags ("+this.flags+") reply is:\n" + this.lfns;
	}
}
