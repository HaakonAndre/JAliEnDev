package alien.catalogue.access;

import alien.catalogue.CatalogEntity;

/**
 * @author ron
 *
 */
public class CatalogueWriteAccess extends CatalogueAccess{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8600686267511296949L;

	/**
	 * Read access to the catalogue object indicated by this GUID.
	 * This constructor is package protected, the objects should be created only by {@link AuthorizationFactory}
	 * @param entity 
	 */
	CatalogueWriteAccess(CatalogEntity entity){
		super(entity);
		
		/// we need to create a new GUID here !!!!!    

		
		
//		
//		this.entity = entity;
//		if(entity.is() == 'l') {
//			lfn = (LFN) entity;
//			guid = (GUID) GUIDUtils.getGUID(lfn.guid);
//		} else {
//			lfn = LFNUtils.getLFN("/NOLFN", true);
//			guid = (GUID) entity;
//		}
//		size = guid.size;
//		md5 = guid.md5;
		
		super.access = WRITE;
	}
}
