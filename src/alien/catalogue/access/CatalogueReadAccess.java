package alien.catalogue.access;

import alien.catalogue.CatalogEntity;

/**
 * @author ron
 *
 */
public class CatalogueReadAccess extends CatalogueAccess{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -144618298447285302L;

	/**
	 * Read access to the catalogue object indicated by this GUID.
	 * This constructor is package protected, the objects should be created only by {@link AuthorizationFactory}
	 * @param entity 
	 */
	CatalogueReadAccess(CatalogEntity entity){
		super(entity);
		super.access = READ;
	}

}
