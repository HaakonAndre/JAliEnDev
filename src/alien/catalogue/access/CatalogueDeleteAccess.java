package alien.catalogue.access;

import alien.catalogue.GUID;

/**
 * @author Steffen
 *
 */
public class CatalogueDeleteAccess extends CatalogueAccess{
	
	/**
	 * Delete access to the catalogue object indicated by this GUID.
	 * This constructor is package protected, the objects should be created only by {@link AuthorizationFactory}
	 * 
	 * @param guid
	 */
	CatalogueDeleteAccess(GUID guid){
		super(guid);
		super.access = "delete";
	}

	@Override
	void decorate(){
		// guid.getALlINFO;
	}
}
