package alien.catalogue.access;

import alien.catalogue.CatalogEntity;
import alien.catalogue.GUID;

/**
 * @author ron
 *
 */
public class CatalogueReadAccess extends CatalogueAccess{
	
	/**
	 * Read access to the catalogue object indicated by this GUID.
	 * This constructor is package protected, the objects should be created only by {@link AuthorizationFactory}
	 * 
	 * @param guid
	 */
	CatalogueReadAccess(CatalogEntity entity){
		super(entity);
		super.access = READ;
	}

}
