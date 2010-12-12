package alien.catalogue.access;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import sun.security.krb5.internal.Ticket;

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
	public static AccessTicket requestAccess(final AliEnPrincipal user,
			final String lfnOrGUID, String access) {

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
				return new AccessTicket(AccessType.DENIED,null);
			
			lfns = guid.getLFNs();
			
			if (lfns!=null && lfns.size()>0)
				lfn = lfns.iterator().next();
		}
		else{
			// is it an LFN ?
			
			// the LFNs are checked to exist only for READ, otherwise we need to create that entry first
			lfn = LFNUtils.getLFN(lfnOrGUID, AccessType.READ.toString() != access);
			
			if (lfn == null)
				return new AccessTicket(AccessType.DENIED,null);
			
			if (lfn.guid != null)
				guid = GUIDUtils.getGUID(lfn.guid);
						
			lfns = new LinkedHashSet<LFN>(1);
			lfns.add(lfn);
		}
		System.out.println("we found lfn: " + lfn.toString());
		System.out.println("we found guid: " + guid.toString());
		
		if (AccessType.READ.toString() == access) {
			if (guid == null)
				return new AccessTicket(AccessType.DENIED,null);
			
			if (!AuthorizationChecker.canRead(guid, user))
				return new AccessTicket(AccessType.DENIED,null);
			
			if (lfns!=null){
				for (LFN lfn1 : lfns){
					if (AuthorizationChecker.canRead(lfn1, user))
						return new AccessTicket(AccessType.READ,lfn);
				}
				
				return new AccessTicket(AccessType.DENIED,null);
			}
			
			return new AccessTicket(AccessType.READ,guid);
		} 
		
		if (AccessType.WRITE.toString() == access) {
			// the object doesn't exist yet, how can we determine if we can write it or not ?
			
			if (lfn == null)
				return new AccessTicket(AccessType.DENIED,null);
			
			if (guid == null){
				// TODO
				// maybe we should generate a new one instead ?
				
				guid = GUIDUtils.createGuid();
				
				return  new AccessTicket(AccessType.DENIED,null);
			}
			
			if (lfn.exists)
				return  new AccessTicket(AccessType.DENIED,null);
			
			LFN parent = lfn.getParentDir();
			
			if (parent==null || parent.getType() != 'd')
				return new AccessTicket(AccessType.DENIED,null);
			
			if (AuthorizationChecker.canWrite(parent, user))
				return new AccessTicket(AccessType.WRITE,lfn);
			
			return new AccessTicket(AccessType.DENIED,null);
		} 
		
		if (AccessType.DELETE.toString() == access) {
			if (guid == null)
				return new AccessTicket(AccessType.DENIED,null);
			
			if (!AuthorizationChecker.canWrite(guid, user))
				return new AccessTicket(AccessType.DENIED,null);
			
			if (lfns!=null){
				// all lfns should be files and the user should be able to delete them
				
				for (LFN lfn1 : lfns){
					if (lfn1.type != 'f')
						new AccessTicket(AccessType.DENIED,null);
					
					LFN parent = lfn1.getParentDir();
					
					if (parent==null || !AuthorizationChecker.canWrite(parent, user))
						new AccessTicket(AccessType.DENIED,null);
				}
				
				return new AccessTicket(AccessType.DELETE,lfn);
			}
		} 
		
		return new AccessTicket(AccessType.WRITE,guid);
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
	private static final ExpirationCache<UUID, AccessTicket> readEnvelopes = new ExpirationCache<UUID, AccessTicket>(
			10000);

}