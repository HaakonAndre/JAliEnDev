package alien.catalogue.access;

import alien.catalogue.CatalogEntity;
import alien.catalogue.GUID;

/**
 * @author ron
 *
 */
public class CatalogueWriteAccess extends CatalogueAccess{
	
	/**
	 * Read access to the catalogue object indicated by this GUID.
	 * This constructor is package protected, the objects should be created only by {@link AuthorizationFactory}
	 * 
	 * @param guid
	 */
	CatalogueWriteAccess(CatalogEntity entity){
		super(entity);
		super.access = WRITE;
	}
}
