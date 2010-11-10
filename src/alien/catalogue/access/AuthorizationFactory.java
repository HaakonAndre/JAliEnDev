package alien.catalogue.access;

import java.util.UUID;

import lazyj.cache.ExpirationCache;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;

/**
 * @author Steffen
 */
public final class AuthorizationFactory {

	/**
	 * Request access to this LFN
	 * 
	 * @param lfn LFN
	 * @param access access type
	 * @return the access, or <code>null</code> if not permitted
	 */
	public static CatalogueAccess requestAccess(final LFN lfn, final String access) {
		return requestAccess(GUIDUtils.getGUID(lfn.guid), access);
	}
	
	/**
	 * Request access to this PFN
	 * 
	 * @param pfn PFN 
	 * @param access access type
	 * @return access, or <code>null</code> if not permitted
	 */
	public static CatalogueAccess requestAccess(final PFN pfn, final String access){
		return requestAccess(pfn.getGuid(), access);
	}

	/**
	 * Request access to this GUID
	 * 
	 * @param guid GUID
	 * @param access access type
	 * @return access, or <code>null</code> if not permitted
	 */
	public static CatalogueAccess requestAccess(final GUID guid, final String access){
		
		if (isAuthorized(guid,access)){
			final CatalogueAccess ca =  accessType(guid, access);
			
			ca.decorate(); // decorate the access with all catalogue info
			
			PFN pfn = ca.pickPFNforAccess(); // however we do that
			
			ca.addEnvelope(new XrootDEnvelope(ca, pfn));
		}
		
		return null;
	}
	
	/**
	 * Check if an user is authorized to access this resource for the indicated operation
	 * 
	 * @param guid
	 * @param access
	 * @return true if allowed
	 */
	public static boolean isAuthorized(final GUID guid, final String access){
		return false;
	}
	
	/**
	 * Cache the recently requested read envelopes 
	 */
	private static final ExpirationCache<UUID, CatalogueAccess> readEnvelopes = new ExpirationCache<UUID, CatalogueAccess>(10000);
	
	private static CatalogueAccess accessType(final GUID guid, final String access){
		
		if (access.equals("delete")){
			return new CatalogueDeleteAccess(guid);
		}
		
		if (access.equals("read")){
			CatalogueAccess readAccess = readEnvelopes.get(guid.guid);
			
			if (readAccess==null){
				readAccess = new CatalogueReadAccess(guid);
				
				readEnvelopes.put(guid.guid, readAccess, 1000*60*5);
			}
		}
		
		return null;
	}

}