/**
 * 
 */
package alien.io.protocols;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.PFN;
import alien.config.ConfigUtils;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Torrent extends Protocol {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Torrent.class.getCanonicalName());
	
	/**
	 * package protected
	 */
	Torrent(){
		// package protected
	}

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
			// we are free to use any cached value
			target = TempFileManager.getAny(pfn.getGuid());
			
			if (target!=null){
				logger.log(Level.FINE, "Reusing cached file: "+target.getCanonicalPath());
				
				return target;
			}
			
			target = File.createTempFile("torrent", null);
		}
		
		String url = pfn.pfn;
		
		// replace torrent:// with http://
		url = "http"+url.substring(url.indexOf("://"));
		
		lazyj.Utils.download(url, target.getCanonicalPath());

		throw new SourceException("Torrent GET method not implemented yet");
		
		// TODO implement downloading the actual content!
		
		// TODO how to check the size and md5sum and so on for a torrent ?
		
//		return target;
	}	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "torrent";
	}

	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#delete(alien.catalogue.PFN)
	 */
	@Override
	public boolean delete(PFN pfn) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#put(alien.catalogue.PFN, java.io.File)
	 */
	@Override
	public String put(PFN pfn, File localFile) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.PFN)
	 */
	@Override
	public String transfer(PFN source, PFN target) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	int getPreference() {
		return 3;
	}
}
