/**
 * 
 */
package alien.io.protocols;

import java.io.File;
import java.io.IOException;

import alien.catalogue.PFN;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Torrent extends Protocol {

	/**
	 * package protected
	 */
	Torrent(){
		// package protected
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7442913364052285097L;

	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#get(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, java.lang.String)
	 */
	@Override
	public File get(final PFN pfn, final File localFile) throws IOException {
		File target = null;

		if (localFile!=null){
			target = localFile;
			
			if (!target.createNewFile())
				throw new IOException("Local file "+localFile+" could not be created");
		}
		
		if (target==null){
			target = File.createTempFile("http", null);
		}
		
		String url = pfn.pfn;
		
		// replace torrent:// with http://
		url = "http"+url.substring(url.indexOf("://"));
		
		lazyj.Utils.download(url, target.getCanonicalPath());
		
		// TODO implement downloading the actual content!
		
		// TODO how to check the size and md5sum and so on for a torrent ?

		return target;
	}	
}
