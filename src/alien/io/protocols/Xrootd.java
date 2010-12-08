/**
 * 
 */
package alien.io.protocols;

import java.io.File;
import java.io.IOException;

import alien.catalogue.PFN;
import alien.catalogue.access.CatalogueReadAccess;
import alien.catalogue.access.CatalogueWriteAccess;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Xrootd extends Protocol {

	/**
	 * package protected
	 */
	Xrootd(){
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
			target = File.createTempFile("xrootd", null);
		}
		
		// TODO implement this
		
		return target;
	}
	
	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#put(alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess, java.lang.String)
	 */
	@Override
	public String put(final PFN pfn, final CatalogueWriteAccess access, final File localFile) throws IOException {
		if (localFile==null || !localFile.exists() || !localFile.isFile() || !localFile.canRead())
			throw new IOException("Local file "+localFile+" cannot be read");
		
		// TODO implement this
		
		return null;
	}
	
	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final CatalogueReadAccess sourceAccess, final PFN target, final CatalogueWriteAccess targetAccess) throws IOException {
		final File temp = get(source, sourceAccess, null);
		
		try{
			return put(target, targetAccess, temp);
		}
		finally{
			temp.delete();
		}
	}
}
