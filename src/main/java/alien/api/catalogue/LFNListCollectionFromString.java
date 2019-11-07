package alien.api.catalogue;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.XmlCollection;
import alien.user.AliEnPrincipal;

/**
 * Get the LFN object for this path
 *
 * @author ron
 * @since Jun 08, 2011
 */
public class LFNListCollectionFromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -7167353294190733455L;
	private final String path;

	private Set<LFN> lfns = null;

	private boolean isXML = false;

	/**
	 * @param user
	 * @param path
	 * @param isXML
	 */
	public LFNListCollectionFromString(final AliEnPrincipal user, final String path, final boolean isXML) {
		setRequestUser(user);
		this.path = path;
		this.isXML = isXML;
	}

	@Override
	public void run() {
		final LFN entry = LFNUtils.getLFN(path, false);

		if (entry != null) {
			if (isXML) {
				try {
					XmlCollection xmlCollection = new XmlCollection(entry);
					this.lfns = new LinkedHashSet<>();

					Iterator<LFN> lfnItr = xmlCollection.iterator();
					while (lfnItr.hasNext()) {
						this.lfns.add(lfnItr.next());
					}

				}
				catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else if (entry.isCollection()) {
				Set<String> entries = entry.listCollection();

				this.lfns = new LinkedHashSet<>(entries.size());

				for (final String lfn : entries)
					this.lfns.add(LFNUtils.getLFN(lfn));
			}
			else
				throw new IllegalArgumentException("Not a collection");
		}
		else
			throw new IllegalArgumentException("No such LFN \"" + path + "\"");
	}

	/**
	 * @return the requested LFN
	 */
	public Set<LFN> getLFNs() {
		return this.lfns;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.path + " reply is: " + this.lfns;
	}
}
