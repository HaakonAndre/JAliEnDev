/**
 * 
 */
package alien.user;

import java.util.Set;

import alien.catalogue.CatalogEntity;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.access.CatalogueAccess;
import alien.catalogue.access.CatalogueAccessDENIED;
import alien.catalogue.access.CatalogueDeleteAccess;
import alien.catalogue.access.CatalogueReadAccess;
import alien.catalogue.access.CatalogueWriteAccess;

/**
 * @author costing
 * @since Nov 11, 2010
 */
public final class AuthorizationChecker {

	private AuthorizationChecker(){
		// utility class
	}
	
	/**
	 * Check if the user owns this entity
	 * 
	 * @param entity
	 * @param user
	 * @return true if the user owns this entity
	 */
	public static boolean isOwner(final CatalogEntity entity, final AliEnPrincipal user){
		return user.canBecome(entity.getOwner());
	}
	
	/**
	 * Check if the user is in the same group as the owner of this file
	 * 
	 * @param entity
	 * @param user
	 * @return true if the user is in the same group
	 */
	public static boolean isGroupOwner(final CatalogEntity entity, final AliEnPrincipal user){
		return user.hasRole(entity.getGroup());
	}

	/**
	 * Get the permission field that applies to the user
	 * 
	 * @param entity
	 * @param user
	 * @return permission field
	 */
	public static int getPermissions(final CatalogEntity entity, final AliEnPrincipal user){
		final Set<String> accounts = user.getNames();
		
		if (accounts!=null && accounts.contains("admin")){
			return 7;
		}
		
		if (user.hasRole("admin")){
			return 7;
		}
		
		if (isOwner(entity, user)){
			return entity.getPermissions().charAt(0) - '0';
		}
		
		if (isGroupOwner(entity, user)){
			return entity.getPermissions().charAt(1) - '0';
		}
		
		return entity.getPermissions().charAt(2) - '0';
	}

	
	
	/**
	 * Check if the user can read the entity
	 * 
	 * @param entity
	 * @param user
	 * @return true if the user can read it
	 */
	public static boolean canRead(final CatalogEntity entity, final AliEnPrincipal user){
		return (getPermissions(entity, user) & 4) == 4;
	}

	/**
	 * Check if the user can write the entity
	 * 
	 * @param entity
	 * @param user
	 * @return true if the user can write it
	 */
	public static boolean canWrite(final CatalogEntity entity, final AliEnPrincipal user){
		return (getPermissions(entity, user) & 2) == 2;
	}
	
	/**
	 * Check if the user can execute the entity 
	 * 
	 * @param entity
	 * @param user
	 * @return true if the user can execute it
	 */
	public static boolean canExecute(final CatalogEntity entity, final AliEnPrincipal user){
		return (getPermissions(entity, user) & 4) == 4;
	}

}
