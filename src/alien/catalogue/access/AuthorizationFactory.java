package alien.catalogue.access;

import java.security.GeneralSecurityException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUID;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import alien.services.XrootDEnvelopeSigner;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * @author ron
 */
public final class AuthorizationFactory {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(AuthorizationFactory.class.getCanonicalName());
	
	/**
	 * Request access to this GUID
	 * @param user 
	 * @param pfn 
	 * @param access 
	 * @return <code>null</code> if access was granted, otherwise the reason why the access was rejected 
	 */
	public static String fillAccess(final AliEnPrincipal user, final PFN pfn, final AccessType access) {

		if (logger.isLoggable(Level.FINE))
			logger.log(Level.FINE, pfn + ", user: "+ user + ", access: " + access);

		final GUID guid = pfn.getGuid();
		
		if (guid==null)
			return "GUID is null for this object";
		
		final Set<PFN> pfns = guid.getPFNs();

		if (access == AccessType.WRITE){
			// PFN must not be part of the ones already registered to the GUID
			
			if (!AuthorizationChecker.canWrite(guid, user))
				return "User is not allowed to write this entry";
			
			if (pfns.contains(pfn))
				return "PFN already associated to the GUID";
		}
		else
		if (access == AccessType.DELETE || access == AccessType.READ) {
			// PFN must be a part of the ones registered to the GUID
			
			if (access==AccessType.DELETE){
				if (!AuthorizationChecker.canWrite(guid, user)){
					return "User is not allowed to delete this entry";
				}
			}
			else{
				if (!AuthorizationChecker.canRead(guid, user)){
					return "User is not allowed to read this entry";
				}
			}
			
			if (!pfns.contains(pfn))
				return "PFN is not registered";
		}
		else
			return "Unknown access type : "+access;
		
		XrootDEnvelope env = null;
		
		if (pfn.getPFN().startsWith("root://")){
			env = new XrootDEnvelope(access, pfn);
			
			try{
				XrootDEnvelopeSigner.signEnvelope(env, false);
		
				final SE se = SEUtils.getSE(pfn.seNumber);
				
				if (se!=null && se.needsEncryptedEnvelope){
					XrootDEnvelopeSigner.encryptEnvelope(env);
				}
			}
			catch (GeneralSecurityException gse){
				logger.log(Level.SEVERE, "Cannot sign and encrypt envelope", gse);
			}

		}
		
		pfn.ticket = new AccessTicket(access, env);
		
		return null;
	}

}