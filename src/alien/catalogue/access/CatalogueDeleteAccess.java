package alien.catalogue.access;

import alien.catalogue.CatalogEntity;

/**
 * @author ron
 *
 */
public class CatalogueDeleteAccess extends CatalogueAccess{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -47906276316932875L;

	/**
	 * Delete access to the catalogue object indicated by this GUID.
	 * This constructor is package protected, the objects should be created only by {@link AuthorizationFactory}
	 * @param entity 
	 */
	CatalogueDeleteAccess(CatalogEntity entity){
		super(entity);
		super.access = DELETE;
	}

}
