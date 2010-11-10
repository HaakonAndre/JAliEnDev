package alien.catalogue.access;

import alien.catalogue.GUID;

/**
 * @author Steffen
 *
 */
public class CatalogueReadAccess extends CatalogueAccess{
	
	/**
	 * Read access to the catalogue object indicated by this GUID.
	 * This constructor is package protected, the objects should be created only by {@link AuthorizationFactory}
	 * 
	 * @param guid
	 */
	CatalogueReadAccess(GUID guid){
		super(guid);
		super.access = "read";
	}

	@Override
	void decorate(){
		// guid.getALlINFO;
	}
}
