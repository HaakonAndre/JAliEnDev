package alien.catalogue;

import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.UUID;

import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * @author costing
 * mimic the register command in AliEn
 */
public class Register {

	/**
	 * @param lfn
	 * @param pfn
	 * @param guid
	 * @param md5
	 * @param size
	 * @param seName
	 * @param user
	 * @return true if everything was ok
	 * @throws IOException
	 */
	public static boolean register(final String lfn, final String pfn, final String guid, final String md5, final long size, final String seName, final AliEnPrincipal user) throws IOException{

		final SE se = SEUtils.getSE(seName);
		
		if (se==null){
			throw new IOException("No such SE "+seName);
		}
		
		final UUID uuid = guid!=null ? UUID.fromString(guid) : GUIDUtils.generateTimeUUID();
			
		final LFN name = LFNUtils.getLFN(lfn, true);
		
		// sanity check
		if (name.exists){
			if (!name.guid.equals(uuid) || name.size!=size || !name.md5.equals(md5))
				throw new IOException("The details don't match the existing LFN fields");
		}
		
		final GUID g = GUIDUtils.getGUID(uuid, true);
		
		// sanity check
		if (g.exists()){
			if (g.size != size || !g.md5.equals(md5))
				throw new IOException("You are trying to associate the wrong entries here");
		}
		
		if (!g.exists()){
			g.ctime = new Date();
			g.md5 = md5;
			g.size = size;
			
			g.owner = user.getName();
			
			final Set<String> roles = user.getRoles();
			
			if (roles!=null && roles.size()>0)
				g.gowner = roles.iterator().next();
			else
				g.gowner = g.owner;
			
			g.type = 0;	// as in the catalogue
			g.perm = "755";
			g.aclId = -1;			
		}
		else{
			if (!AuthorizationChecker.canWrite(g, user))
				throw new IOException("User "+user.getName()+" cannot update GUID "+uuid);
		}
		
		if (!g.seStringList.contains(Integer.valueOf(se.seNumber))){
			PFN p = new PFN(g, se);
			p.pfn = pfn;
			
			if (!g.addPFN(p))
				return false;
		}
		
		if (!name.exists){
			LFN check = name.getParentDir();
			
			while (check!=null && !check.exists ){
				check = check.getParentDir();
			}
			
			if (!AuthorizationChecker.canWrite(check, user)){
				throw new IOException("User "+user.getName()+" is not allowed to write LFN "+lfn);
			}
			
			name.size = g.size;
			name.owner = g.owner;
			name.gowner = g.gowner;
			name.perm = g.perm;
			name.aclId = g.aclId;
			name.ctime = g.ctime;
			name.expiretime = g.expiretime;
			name.guid = g.guid;
			// lfn.guidtime = ?;
			
			name.md5 = g.md5;
			name.type = g.type;
			
			name.guidtime = Long.toHexString(GUIDUtils.epochTime(g.guid));
			
			name.jobid = -1;
			
			return LFNUtils.insertLFN(name);
		}
		
		return true;
	}
	
}
