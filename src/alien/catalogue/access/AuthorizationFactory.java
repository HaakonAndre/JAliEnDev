package alien.catalogue.access;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import lazyj.cache.ExpirationCache;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * @author ron
 */
public final class AuthorizationFactory {

	/**
	 * Request access to this GUID
	 * @param user 
	 * @param lfnOrGUID 
	 * @param access
	 *            access type
	 * @return access, or <code>null</code> if not permitted
	 */
	public static CatalogueAccess requestAccess(final AliEnPrincipal user,
			final String lfnOrGUID, final int access) {

		System.out.println("i got: lfn:" + lfnOrGUID + " for user: "+ user.toString() + " access: " + access);
		
		UUID uuid = null;
		
		try{
			uuid = UUID.fromString(lfnOrGUID);
		}
		catch (IllegalArgumentException iae){
			// ignore
		}
		
		GUID guid = null;
		
		Set<LFN> lfns = null;
		
		LFN lfn = null;
		
		if (uuid!=null){
			// starting from a GUID
			
			guid = GUIDUtils.getGUID(uuid);
			
			if (guid == null)
				return new CatalogueAccessDENIED(lfnOrGUID);
			
			lfns = guid.getLFNs();
			
			if (lfns!=null && lfns.size()>0)
				lfn = lfns.iterator().next();
		}
		else{
			// is it an LFN ?
			
			// the LFNs are checked to exist only for READ, otherwise we need to create that entry first
			lfn = LFNUtils.getLFN(lfnOrGUID, access == CatalogueAccess.READ ? false : true);
			
			if (lfn == null)
				return new CatalogueAccessDENIED(lfnOrGUID);
			
			if (lfn.guid != null)
				guid = GUIDUtils.getGUID(lfn.guid);
						
			lfns = new LinkedHashSet<LFN>(1);
			lfns.add(lfn);
		}
		
		if (access == CatalogueAccess.READ) {
			if (guid == null)
				return new CatalogueAccessDENIED(lfnOrGUID);
			
			if (!AuthorizationChecker.canRead(guid, user))
				return new CatalogueAccessDENIED(guid);
			
			if (lfns!=null){
				for (LFN lfn1 : lfns){
					if (AuthorizationChecker.canRead(lfn1, user))
						return new CatalogueReadAccess(guid);
				}
				
				return new CatalogueAccessDENIED(guid);
			}
			
			return new CatalogueReadAccess(guid);
		} 
		
		if (access == CatalogueAccess.WRITE) {
			// the object doesn't exist yet, how can we determine if we can write it or not ?
			
			if (lfn == null)
				return new CatalogueAccessDENIED(lfnOrGUID);
			
			if (guid == null){
				// TODO
				// maybe we should generate a new one instead ?
				
				guid = GUIDUtils.createGuid();
				
				return new CatalogueAccessDENIED(lfnOrGUID);
			}
			
			if (lfn.exists)
				return new CatalogueAccessDENIED(lfnOrGUID);
			
			LFN parent = lfn.getParentDir();
			
			if (parent==null || parent.getType() != 'd')
				return new CatalogueAccessDENIED(lfnOrGUID);
			
			if (AuthorizationChecker.canWrite(parent, user))
				return new CatalogueWriteAccess(lfn);
			
			return new CatalogueAccessDENIED(lfn);
		} 
		
		if (access == CatalogueAccess.DELETE) {
			if (guid == null)
				return new CatalogueAccessDENIED(lfnOrGUID);
			
			if (!AuthorizationChecker.canWrite(guid, user))
				return new CatalogueAccessDENIED(guid);
			
			if (lfns!=null){
				// all lfns should be files and the user should be able to delete them
				
				for (LFN lfn1 : lfns){
					if (lfn1.type != 'f')
						return new CatalogueAccessDENIED(lfn1);
					
					LFN parent = lfn1.getParentDir();
					
					if (parent==null || !AuthorizationChecker.canWrite(parent, user))
						return new CatalogueAccessDENIED(lfn1);
				}
				
				return new CatalogueDeleteAccess(guid);
			}
		} 
		
		return new CatalogueAccessDENIED(lfnOrGUID);
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

	private static CatalogueAccess accessType(final GUID guid, final String access) {

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