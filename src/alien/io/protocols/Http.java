/**
 * 
 */
package alien.io.protocols;

import java.io.File;
import java.io.IOException;

import alien.catalogue.PFN;
import alien.catalogue.access.CatalogueReadAccess;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Http extends Protocol {

	/**
	 * package protected
	 */
	Http(){
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
	public File get(final PFN pfn, final CatalogueReadAccess access, final File localFile) throws IOException {
		File target = null;

		if (localFile!=null){
			target = localFile;
			
			if (!target.createNewFile())
				throw new IOException("Local file "+localFile+" could not be created");
		}
		
		if (target==null){
			target = File.createTempFile("http", null);
		}
		
		try{
			lazyj.Utils.download(pfn.pfn, target.getCanonicalPath());
			
			if (!checkDownloadedFile(target, access))
				throw new IOException("Local file doesn't match catalogue details");
		}
		catch (IOException ioe){
			target.delete();
			
			throw ioe;
		}
		
		return target;
	}	
}
