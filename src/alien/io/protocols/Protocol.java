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
public abstract class Protocol {
	
	/**
	 * Package protected 
	 */
	Protocol(){
		// package protected
	}
	
	/**
	 * Download the file locally
	 * 
	 * @param pfn location
	 * @param access envelope
	 * @param localFile local file name (can be <code>null</code> to generate a temporary file name)
	 * @return local file name
	 * @throws IOException in case of problems
	 */
	public File get(final PFN pfn, final CatalogueReadAccess access, final File localFile) throws IOException{
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Upload the local file
	 * 
	 * @param pfn target PFN
	 * @param access envelope
	 * @param localFile local file name (which must exist of course)
	 * @return storage reply envelope
	 * @throws IOException in case of problems
	 */
	public String put(final PFN pfn, final CatalogueWriteAccess access, final File localFile) throws IOException{
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Directly transfer between two
	 * 
	 * @param source
	 * @param sourceAccess
	 * @param target
	 * @param targetAccess
	 * @return storage reply envelope
	 * @throws IOException
	 */
	public String transfer(final PFN source, final CatalogueReadAccess sourceAccess, final PFN target, final CatalogueWriteAccess targetAccess) throws IOException{
		throw new UnsupportedOperationException();
	}
	
}
