package alien.catalogue;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import lazyj.DBFunctions;

import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * @author costing
 *
 */
public class BookingTable {

	/**
	 * @param user
	 * @param lfn
	 * @param requestedGUID
	 * @param jobid
	 * @param se
	 * @return the PFN with the access envelope if everything is ok
	 * @throws IOException
	 */
	public PFN bookForWriting(final AliEnPrincipal user, final LFN lfn, final UUID requestedGUID, final int jobid, final SE se) throws IOException {
		if (lfn==null){
			throw new IllegalArgumentException("LFN cannot be null");
		}
		
		if (user==null){
			throw new IllegalArgumentException("Principal cannot be null");
		}

		if (!AuthorizationChecker.canWrite(lfn, user)){
			throw new IOException("User "+user.getName()+" is not allowed to write LFN "+lfn.getCanonicalName());
		}
		
		if (se==null){
			throw new IllegalArgumentException("SE cannot be null");
		}
		
		final DBFunctions db = ConfigUtils.getDB("alien_system");
	
		final UUID u = requestedGUID != null ? requestedGUID : GUIDUtils.generateTimeUUID();
		
		GUID guid = GUIDUtils.getGUID(u);
		
		if (guid!=null){
			// the GUID is already booked in the catalogue
			// first question, is the user allowed to write it ?
			if (!AuthorizationChecker.canWrite(guid, user))
				throw new IOException("User "+user.getName()+" is not allowed to write GUID "+guid);
			
			// check if there isn't a replica already on this storage element
			final Set<PFN> pfns = guid.getPFNs();
			
			if (pfns!=null){
				for (final PFN pfn: pfns)
					if (pfn.seNumber == se.seNumber)
						throw new IOException("This GUID already has a replica in the requested SE");
			}
		}
		else
			guid = GUIDUtils.getGUID(u, true);

		final PFN pfn = new PFN(guid, se);
		
		// now check the booking table for previous attempts
		db.query("SELECT owner FROM LFN_BOOKED WHERE guid=string2binary('"+guid.guid.toString()+"') AND se='"+se.getName()+"' AND expiretime>0;");
		
		if (db.moveNext()){
			// there is a previous attempt on this GUID to this SE, who is the owner?
			if (db.gets(1).equals(user.getName())){
				// that's fine, it's the same user, we can recycle the entry
				// TODO
				db.query("UPDATE LFN_BOOKED SET expiretime=? WHERE guid=string2binary('"+guid.guid.toString()+"') AND se='"+se.getName()+"'");
			}
			else
				throw new IOException("You are not allowed to do this");
		}
		else{
			// TODO : create the entry in the booking table
			db.query("");
		}
		
		final String reason = AuthorizationFactory.fillAccess(user, pfn, AccessType.WRITE);
		
		if (reason!=null)
			throw new IOException("Access denied: "+reason);
		
		return pfn;
	}
}
