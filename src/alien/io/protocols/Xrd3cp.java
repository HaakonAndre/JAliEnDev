/**
 * 
 */
package alien.io.protocols;

import java.io.IOException;

import alien.catalogue.PFN;
import alien.catalogue.access.CatalogueReadAccess;
import alien.catalogue.access.CatalogueWriteAccess;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Xrd3cp extends Xrootd {

	/**
	 * package protected
	 */
	Xrd3cp(){
		// package protected
	}
	
	/* (non-Javadoc)
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final CatalogueReadAccess sourceAccess, final PFN target, final CatalogueWriteAccess targetAccess) throws IOException {
		// direct copying between two storages
		
		// TODO implement this
		
		return null;
	}
}
