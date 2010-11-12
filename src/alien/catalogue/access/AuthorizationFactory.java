package alien.catalogue.access;

import java.util.UUID;

import lazyj.cache.ExpirationCache;
import alien.catalogue.CatalogEntity;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import alien.user.UserFactory;

/**
 * @author ron
 */
public final class AuthorizationFactory {

	/**
	 * Request access to this GUID
	 * 
	 * @param guid
	 *            GUID
	 * @param access
	 *            access type
	 * @return access, or <code>null</code> if not permitted
	 */
	public static CatalogueAccess requestAccess(final AliEnPrincipal user, final CatalogEntity entity, final int access){
		
		CatalogueAccess ca = null;
		
		if(access == CatalogueAccess.READ){

				if(entity.is() == 'l'){
					if( AuthorizationChecker.canRead(((LFN) entity).getParentDir(), user) &&		
					AuthorizationChecker.canRead(((LFN) entity), user) &&
					AuthorizationChecker.canRead(((GUID) GUIDUtils.getGUID(( (LFN) entity).guid )), user) ){
						ca = new CatalogueReadAccess(entity);
					} else {
						ca =  new CatalogueAccessDENIED(entity);
					}
				} else
				{
					if(AuthorizationChecker.canRead(entity, user)  ){
								ca = new CatalogueReadAccess(entity);
							} else {
								ca =  new CatalogueAccessDENIED(entity);
							}
				}
		} else	if(access == CatalogueAccess.WRITE){			
				if(entity.is() == 'l'){
					if( AuthorizationChecker.canRead(((LFN) entity).getParentDir(), user) &&
						// AuthorizationChecker.canWrite(((LFN) entity).getParentDir(), user) &&		
					AuthorizationChecker.isOwner(entity, user) &&
					AuthorizationChecker.canWrite(entity, user) &&
					AuthorizationChecker.isOwner(((GUID) GUIDUtils.getGUID(( (LFN) entity).guid )), user) &&
					AuthorizationChecker.canWrite(((GUID) GUIDUtils.getGUID(( (LFN) entity).guid )), user) 
					){
						ca = new CatalogueWriteAccess(entity);
					} else {
						ca =  new CatalogueAccessDENIED(entity);
					}
				} else
				{
					if(AuthorizationChecker.canWrite( entity, user) &&
							AuthorizationChecker.isOwner( entity, user) ){
						ca = new CatalogueWriteAccess(entity);
							} else {
								ca =  new CatalogueAccessDENIED(entity);
							}
				}
		} else	if(access == CatalogueAccess.DELETE){		
				if(entity.is() == 'l'){
					if( AuthorizationChecker.canRead(((LFN) entity).getParentDir(), user) &&
						AuthorizationChecker.canWrite(((LFN) entity).getParentDir(), user) &&		
					AuthorizationChecker.isOwner(entity, user) &&
					AuthorizationChecker.canWrite(entity, user) &&
					AuthorizationChecker.isOwner(((GUID) GUIDUtils.getGUID(( (LFN) entity).guid )), user) &&
					AuthorizationChecker.canWrite(((GUID) GUIDUtils.getGUID(( (LFN) entity).guid )), user) 
					){
						ca = new CatalogueDeleteAccess(entity);
					} else {
						ca =  new CatalogueAccessDENIED(entity);
					}
				} else
				{
					if(AuthorizationChecker.canWrite( entity, user) &&
							AuthorizationChecker.isOwner( entity, user) ){
						ca = new CatalogueDeleteAccess(entity);
							} else {
								ca =  new CatalogueAccessDENIED(entity);
							}
				}
		} else	{		
				ca = new CatalogueAccessDENIED(entity);
		}			
		return ca;
	}

	/**
	 * Check if an user is authorized to access this resource for the indicated
	 * operation
	 * 
	 * @param guid
	 * @param access
	 * @return true if allowed
	 */
	public static boolean isAuthorized(final GUID guid, final String access) {
		return false;
	}

	/**
	 * Cache the recently requested read envelopes
	 */
	private static final ExpirationCache<UUID, CatalogueAccess> readEnvelopes = new ExpirationCache<UUID, CatalogueAccess>(
			10000);

	private static CatalogueAccess accessType(final GUID guid,
			final String access) {

		if (access.equals("delete")) {
			return new CatalogueDeleteAccess(guid);
		}

		if (access.equals("read")) {
			CatalogueAccess readAccess = readEnvelopes.get(guid.guid);

			if (readAccess == null) {
				readAccess = new CatalogueReadAccess(guid);

				readEnvelopes.put(guid.guid, readAccess, 1000 * 60 * 5);
			}
		}

		return null;
	}

}