package alien.catalogue.access;

import alien.catalogue.CatalogEntity;

/**
 * @author ron
 *
 */
public class CatalogueAccessDENIED extends CatalogueAccess{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4077950097481610194L;
	
	private String name;
	
	/**
	 * Delete access to the catalogue object indicated by this GUID.
	 * This constructor is package protected, the objects should be created only by {@link AuthorizationFactory}
	 * @param entity 
	 */
	CatalogueAccessDENIED(final CatalogEntity entity){
		super(null);
		name = entity.getName();
		super.access = INVALID;
	}
	
	/**
	 * @param name the entry (of unknown type) to which the access was denied
	 */
	CatalogueAccessDENIED(final String name){
		super(null);
		this.name = name;
		super.access = INVALID;
	}
	
	/**
	 * @return the entity name for which access was denied
	 */
	public String getName(){
		return name;
	}

	@Override
	public void addEnvelope(final XrootDEnvelope envelope){
		// don't allow this
	}
}
