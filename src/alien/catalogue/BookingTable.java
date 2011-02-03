package alien.catalogue;

import java.io.IOException;
import java.util.Set;

import lazyj.DBFunctions;
import lazyj.Format;
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

	private static final DBFunctions getDB(){
		return ConfigUtils.getDB("alice_users");
	}
	
	/**
	 * @param user
	 * @param lfn
	 * @param requestedGUID
	 * @param requestedPFN 
	 * @param jobid
	 * @param se
	 * @return the PFN with the write access envelope if allowed to write or <code>null</code>
	 * 			if the PFN doesn't indicate a physical file but the entry was successfully booked
	 * @throws IOException if now allowed to do that
	 */
	public static PFN bookForWriting(final AliEnPrincipal user, final LFN lfn, final GUID requestedGUID, final PFN requestedPFN, final int jobid, final SE se) throws IOException {
		if (lfn==null)
			throw new IllegalArgumentException("LFN cannot be null");
		
		if (user==null)
			throw new IllegalArgumentException("Principal cannot be null");

		if (se==null)
			throw new IllegalArgumentException("SE cannot be null");

		if (requestedGUID==null)
			throw new IllegalArgumentException("requested GUID cannot be null");
		
		if (!AuthorizationChecker.canWrite(lfn, user)){
			throw new IOException("User "+user.getName()+" is not allowed to write LFN "+lfn.getCanonicalName());
		}
		
		final DBFunctions db = getDB();
	
		// check if the GUID is already booked in the catalogue
		GUID checkGUID = GUIDUtils.getGUID(requestedGUID.guid);
		
		if (checkGUID!=null){
			// first question, is the user allowed to write it ?
			if (!AuthorizationChecker.canWrite(checkGUID, user))
				throw new IOException("User "+user.getName()+" is not allowed to write GUID "+checkGUID);
			
			// check if there isn't a replica already on this storage element
			final Set<PFN> pfns = checkGUID.getPFNs();
			
			if (pfns!=null){
				for (final PFN pfn: pfns)
					if (pfn.seNumber == se.seNumber)
						throw new IOException("This GUID already has a replica in the requested SE");
			}
		}
		
		if (requestedPFN!=null){
			// TODO should we check whether or not this PFN exists? It's a heavy op ... 
		}
		
		final PFN pfn = requestedPFN != null ? requestedPFN : new PFN(requestedGUID, se);
		
		// delete previous failed attempts since we are overwriting this pfn
		db.query("DELETE FROM LFN_BOOKED WHERE guid=string2binary('"+requestedGUID.guid.toString()+"') AND se='"+Format.escSQL(se.getName())+"' AND pfn='"+Format.escSQL(pfn.getPFN())+"' AND expiretime<0;");
		
		// now check the booking table for previous attempts
		db.query("SELECT owner FROM LFN_BOOKED WHERE guid=string2binary('"+requestedGUID.guid.toString()+"') AND se='"+Format.escSQL(se.getName())+"' AND pfn='"+Format.escSQL(pfn.getPFN())+"' AND expiretime>0;");
		
		if (db.moveNext()){
			// there is a previous attempt on this GUID to this SE, who is the owner?
			if (user.canBecome(db.gets(1))){
				final String reason = AuthorizationFactory.fillAccess(user, pfn, AccessType.WRITE);
				
				if (reason!=null)
					throw new IOException("Access denied: "+reason);
				
				// that's fine, it's the same user, we can recycle the entry
				db.query("UPDATE LFN_BOOKED SET expiretime=unix_timestamp(now())+86400 WHERE guid=string2binary('"+requestedGUID.guid.toString()+"') AND se='"+Format.escSQL(se.getName())+"' AND pfn='"+Format.escSQL(pfn.getPFN())+"'");
			}
			else
				throw new IOException("You are not allowed to do this");
		}
		else{
			final String reason = AuthorizationFactory.fillAccess(user, pfn, AccessType.WRITE);
			
			if (reason!=null)
				throw new IOException("Access denied: "+reason);
			
			// create the entry in the booking table
			final StringBuilder q = new StringBuilder("INSERT INTO LFN_BOOKED (lfn,owner,md5sum,expiretime,size,pfn,se,gowner,user,guid,jobid) VALUES ("); 
			
			q.append(e(lfn.getCanonicalName())).append(',');	// LFN
			q.append(e(user.getName())).append(',');			// owner
			q.append(e(requestedGUID.md5)).append(',');			// md5sum
			q.append("unix_timestamp(now())+86400,");			// expiretime, 24 hours from now
			q.append(requestedGUID.size).append(',');			// size
			q.append(e(pfn.getPFN())).append(',');				// pfn
			q.append(e(se.getName())).append(',');				// SE
			
			final Set<String> roles = user.getRoles();
			
			if (roles!=null && roles.size()>0)
				q.append(e(roles.iterator().next()));
			else
				q.append("null");
			
			q.append(',');		// gowner
			q.append(e(user.getName())).append(',');			// user
			q.append("string2binary('"+requestedGUID.guid.toString()+"'),");	// guid
			
			if (jobid>0)
				q.append(jobid);
			else
				q.append("null");
			
			q.append(");");
			
			db.query(q.toString());
		}
				
		return pfn;
	}
	
	private static final String e(final String s){
		if (s!=null)
			return "'"+Format.escSQL(s)+"'";

		return "null";
	}
}
