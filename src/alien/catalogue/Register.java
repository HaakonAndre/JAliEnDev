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
 * @author costing mimic the register command in AliEn
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
	public static boolean register(final String lfn, final String pfn, final String guid, final String md5, final long size, final String seName, final AliEnPrincipal user) throws IOException {
		return register(lfn, pfn, guid, md5, size, seName, user, -1);
	}

	/**
	 * @param lfn
	 * @param pfn
	 * @param guid
	 * @param md5
	 * @param size
	 * @param seName
	 * @param user
	 * @param jobId 
	 * @return true if everything was ok
	 * @throws IOException
	 */
	public static boolean register(final String lfn, final String pfn, final String guid, final String md5, final long size, final String seName, final AliEnPrincipal user, final long jobId)
			throws IOException {

		final SE se = SEUtils.getSE(seName);

		if (se == null) {
			throw new IOException("No such SE " + seName);
		}

		final UUID uuid = guid != null && guid.length() > 0 ? UUID.fromString(guid) : GUIDUtils.generateTimeUUID();

		final LFN name = LFNUtils.getLFN(lfn, true);

		// sanity check
		if (name.exists) {
			if (!name.guid.equals(uuid) || name.size != size || !name.md5.equals(md5))
				throw new IOException("The details don't match the existing LFN fields");
		} else {
			LFN check = name.getParentDir();

			while (check != null && !check.exists) {
				check = check.getParentDir();
			}

			if (!AuthorizationChecker.canWrite(check, user)) {
				throw new IOException("User " + user.getName() + " is not allowed to write LFN " + lfn);
			}
		}

		GUID g = GUIDUtils.getGUID(uuid, true);

		// sanity check
		if (g.exists()) {
			if (g.size != size || !g.md5.equals(md5)) {
				System.err.println("Register : GUID exists for " + uuid + " and the details don't match:\n" + g + "\nwhile the new size = " + size + " and the md5 is " + md5);

				final Set<PFN> pfns = g.getPFNs();

				if (pfns == null || pfns.size() == 0) {
					System.err.println("Register : no pfns associated to " + uuid);
				} else {
					for (final PFN pfnit : pfns) {
						System.err.println("Register : " + uuid + " - associated pfn : " + pfnit.pfn);
					}
				}

				final Set<LFN> lfns = g.getLFNs();

				if (lfns == null || lfns.size() == 0) {
					System.err.println("Register : no lfns associated to " + uuid);
				} else {
					for (final LFN lfnit : lfns) {
						System.err.println("Register : " + uuid + " - associated lfn : " + lfnit.getCanonicalName());
					}
				}

				g = GUIDUtils.createGuid();

				System.err.println("Register : replacing " + uuid + " with " + g.guid + " because of the conflict");

				// throw new IOException("You are trying to associate the wrong entries here ("+g.size+", "+g.md5+") != ("+size+", "+md5+")");
			} else if (!AuthorizationChecker.canWrite(g, user))
				throw new IOException("User " + user.getName() + " cannot update GUID " + uuid);
		}

		if (!g.exists()) {
			g.ctime = new Date();
			g.md5 = md5;
			g.size = size;

			g.owner = user.getName();

			final Set<String> roles = user.getRoles();

			if (roles != null && roles.size() > 0)
				g.gowner = roles.iterator().next();
			else
				g.gowner = g.owner;

			g.type = 0; // as in the catalogue
			g.perm = "755";
			g.aclId = -1;
		}

		if (!g.seStringList.contains(Integer.valueOf(se.seNumber))) {
			PFN p = new PFN(g, se);
			p.pfn = pfn;

			if (!g.addPFN(p)) {
				return false;
			}
		}

		if (!name.exists) {
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
			name.type = 'f';

			name.guidtime = GUIDUtils.getIndexTime(g.guid);

			name.jobid = jobId;

			final boolean insertOK = LFNUtils.insertLFN(name);

			return insertOK;
		}

		return true;
	}

}
