package alien.catalogue;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.quotas.FileQuota;
import alien.quotas.QuotaUtilities;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.StringFactory;

/**
 * @author costing
 *
 */
public class BookingTable {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(BookingTable.class.getCanonicalName());

	private static final DBFunctions getDB() {
		final DBFunctions db = ConfigUtils.getDB("alice_users");
		db.setQueryTimeout(600);
		return db;
	}

	/**
	 * @param lfn
	 * @param requestedGUID
	 * @param requestedPFN
	 * @param se
	 * @return the PFN with the write access envelope if allowed to write or <code>null</code> if the PFN doesn't indicate a physical file but the entry was successfully booked
	 * @throws IOException
	 *             if not allowed to do that
	 */
	public static PFN bookForWriting(final LFN lfn, final GUID requestedGUID, final PFN requestedPFN, final SE se) throws IOException {
		return bookForWriting(AuthorizationFactory.getDefaultUser(), lfn, requestedGUID, requestedPFN, se);
	}

	/**
	 * @param user
	 *            <code>null</code> not allowed
	 * @param lfn
	 *            <code>null</code> not allowed
	 * @param requestedGUID
	 *            <code>null</code> not allowed
	 * @param requestedPFN
	 *            can be <code>null</code> and then a PFN specific for this SE and this GUID is generated
	 * @param se
	 *            <code>null</code> not allowed
	 * @return the PFN with the write access envelope if allowed to write or <code>null</code> if the PFN doesn't indicate a physical file but the entry was successfully booked
	 * @throws IOException
	 *             if not allowed to do that
	 */
	public static PFN bookForWriting(final AliEnPrincipal user, final LFN lfn, final GUID requestedGUID, final PFN requestedPFN, final SE se) throws IOException {
		if (lfn == null)
			throw new IllegalArgumentException("LFN cannot be null");

		if (user == null)
			throw new IllegalArgumentException("Principal cannot be null");

		if (se == null)
			throw new IllegalArgumentException("SE cannot be null");

		if (!se.canWrite(user))
			throw new IllegalArgumentException("SE doesn't allow " + user.getName() + " to write there");

		if (requestedGUID == null)
			throw new IllegalArgumentException("requested GUID cannot be null");

		LFN check = lfn;

		if (!check.exists)
			check = check.getParentDir();

		if (!AuthorizationChecker.canWrite(check, user)) {
			String message = "User " + user.getName() + " is not allowed to write LFN " + lfn.getCanonicalName();

			if (check == null)
				message += ": no such folder " + lfn.getParentName();
			else
				if (!check.equals(lfn))
					message += ": not enough rights on " + check.getCanonicalName();

			throw new IOException(message);
		}

		try (DBFunctions db = getDB()) {
			// check if the GUID is already booked in the catalogue
			final GUID checkGUID = GUIDUtils.getGUID(requestedGUID.guid);

			if (checkGUID != null) {
				// first question, is the user allowed to write it ?
				if (!AuthorizationChecker.canWrite(checkGUID, user))
					throw new IOException("User " + user.getName() + " is not allowed to write GUID " + checkGUID);

				// check if there isn't a replica already on this storage element
				final Set<PFN> pfns = checkGUID.getPFNs();

				if (pfns != null)
					for (final PFN pfn : pfns)
						if (se.equals(pfn.getSE()))
							throw new IOException("This GUID already has a replica in the requested SE");
			}
			else {
				// check the file quota only for new files, extra replicas don't count towards the quota limit

				final FileQuota quota = QuotaUtilities.getFileQuota(requestedGUID.owner);

				if (quota != null && !quota.canUpload(1, requestedGUID.size))
					throw new IOException("User " + requestedGUID.owner + " has exceeded the file quota and is not allowed to write any more files");
			}

			if (requestedPFN != null) {
				// TODO should we check whether or not this PFN exists? It's a heavy op ...
			}

			final PFN pfn = requestedPFN != null ? requestedPFN : new PFN(requestedGUID, se);

			pfn.setGUID(requestedGUID);

			// delete previous failed attempts since we are overwriting this pfn
			db.query("DELETE FROM LFN_BOOKED WHERE guid=string2binary(?) AND se=? AND pfn=? AND expiretime<0;", false, requestedGUID.guid.toString(), se.getName(), pfn.getPFN());

			// now check the booking table for previous attempts
			db.setReadOnly(true);
			db.query("SELECT owner FROM LFN_BOOKED WHERE guid=string2binary(?) AND se=? AND pfn=? AND expiretime>0;", false, requestedGUID.guid.toString(), se.getName(), pfn.getPFN());
			db.setReadOnly(false);

			if (db.moveNext()) {
				// there is a previous attempt on this GUID to this SE, who is the owner?
				if (user.canBecome(db.gets(1))) {
					final String reason = AuthorizationFactory.fillAccess(user, pfn, AccessType.WRITE);

					if (reason != null)
						throw new IOException("Access denied: " + reason);

					// that's fine, it's the same user, we can recycle the entry
					db.query("UPDATE LFN_BOOKED SET expiretime=unix_timestamp(now())+86400 WHERE guid=string2binary(?) AND se=? AND pfn=?;", false, requestedGUID.guid.toString(), se.getName(),
							pfn.getPFN());
				}
				else
					throw new IOException("You are not allowed to do this");
			}
			else {
				// make sure a previously queued deletion request for this file is wiped before giving out a new token
				db.query("DELETE FROM orphan_pfns WHERE guid=string2binary(?) AND se=?;", false, requestedGUID.guid.toString(), Integer.valueOf(se.seNumber));
				db.query("DELETE FROM orphan_pfns_" + se.seNumber + " WHERE guid=string2binary(?);", true, requestedGUID.guid.toString());

				final String reason = AuthorizationFactory.fillAccess(user, pfn, AccessType.WRITE);

				if (reason != null)
					throw new IOException("Access denied: " + reason);

				// create the entry in the booking table
				final StringBuilder q = new StringBuilder("INSERT INTO LFN_BOOKED (lfn,owner,md5sum,expiretime,size,pfn,se,gowner,user,guid,jobid) VALUES (");

				String lfnName = lfn.getCanonicalName();

				if (lfnName.equalsIgnoreCase("/" + requestedGUID.guid.toString()))
					lfnName = "";

				q.append(e(lfnName)).append(','); // LFN
				q.append(e(user.getName())).append(','); // owner
				q.append(e(requestedGUID.md5)).append(','); // md5sum
				q.append("unix_timestamp(now())+86400,"); // expiretime, 24 hours from now
				q.append(requestedGUID.size).append(','); // size
				q.append(e(pfn.getPFN())).append(','); // pfn
				q.append(e(se.getName())).append(','); // SE

				final Set<String> roles = user.getRoles();

				if (roles != null && roles.size() > 0)
					q.append(e(roles.iterator().next()));
				else
					q.append("null");

				q.append(','); // gowner
				q.append(e(user.getName())).append(','); // user
				q.append("string2binary('" + requestedGUID.guid.toString() + "'),"); // guid

				if (lfn.jobid > 0)
					q.append(lfn.jobid);
				else
					q.append("null");

				q.append(");");

				db.query(q.toString());
			}

			return pfn;
		}
	}

	/**
	 * Promote this entry to the catalog
	 *
	 * @param user
	 * @param pfn
	 * @return true if successful, false if not
	 */
	public static LFN commit(final AliEnPrincipal user, final PFN pfn) {
		return mark(user, pfn, true);
	}

	/**
	 * Mark this entry as failed, to be recycled
	 *
	 * @param user
	 * @param pfn
	 * @return true if marking was ok, false if not
	 */
	public static boolean reject(final AliEnPrincipal user, final PFN pfn) {
		return mark(user, pfn, false) != null;
	}

	private static LFN mark(final AliEnPrincipal user, final PFN pfn, final boolean ok) {
		LFN ret = null;

		try (DBFunctions db = getDB()) {
			if (user == null) {
				logger.log(Level.WARNING, "Not marking since the user is null");
				return null;
			}

			if (pfn == null) {
				logger.log(Level.WARNING, "Not marking since the PFN is null");
				return null;
			}

			String w = "pfn" + eq(pfn.getPFN());

			final SE se = pfn.getSE();

			if (se == null) {
				logger.log(Level.WARNING, "Not marking since there is no valid SE in this PFN: " + pfn);
				return null;
			}

			w += " AND se" + eq(se.getName());

			final GUID guid = pfn.getGuid();

			if (guid == null) {
				logger.log(Level.WARNING, "Not marking since there is no GUID in this PFN: " + pfn);
				return null;
			}

			w += " AND guid=string2binary(" + e(guid.guid.toString()) + ")";

			w += " AND owner" + eq(user.getName());

			if (!ok) {
				db.query("UPDATE LFN_BOOKED SET expiretime=-1*(unix_timestamp(now())+60*60*24*30) WHERE " + w);
				return new LFN("/bogus");
			}

			if (!guid.addPFN(pfn))
				if (guid.hasReplica(pfn.seNumber))
					logger.log(Level.FINE, "Could not add the PFN to this GUID: " + guid + "\nPFN: " + pfn);
				else {
					logger.log(Level.WARNING, "Could not add the PFN to this GUID: " + guid + "\nPFN: " + pfn);
					return null;
				}

			db.setReadOnly(true);

			db.query("SELECT lfn,jobid FROM LFN_BOOKED WHERE " + w);

			db.setReadOnly(false);

			while (db.moveNext()) {
				final String sLFN = db.gets(1);

				if (sLFN.length() == 0)
					continue;

				final LFN lfn = LFNUtils.getLFN(sLFN, true);

				if (lfn.exists)
					ret = lfn;
				else {
					lfn.size = guid.size;
					lfn.owner = guid.owner;
					lfn.gowner = guid.gowner;
					lfn.perm = guid.perm;
					lfn.aclId = guid.aclId;
					lfn.ctime = guid.ctime;
					lfn.expiretime = guid.expiretime;
					lfn.guid = guid.guid;
					// lfn.guidtime = ?;

					lfn.md5 = guid.md5;
					lfn.type = guid.type != 0 ? guid.type : 'f';

					lfn.guidtime = GUIDUtils.getIndexTime(guid.guid);

					lfn.jobid = db.getl(2, -1);

					final boolean inserted = LFNUtils.insertLFN(lfn);

					if (!inserted) {
						logger.log(Level.WARNING, "Could not insert this LFN in the catalog : " + lfn);
						return null;
					}

					ret = lfn;
				}
			}

			// was booked, now let's move it to the catalog
			db.query("DELETE FROM LFN_BOOKED WHERE " + w);
		}

		return ret;
	}

	private static final String eq(final String s) {
		if (s == null)
			return " IS NULL";

		return "='" + Format.escSQL(s) + "'";
	}

	private static final String e(final String s) {
		if (s != null)
			return "'" + Format.escSQL(s) + "'";

		return "null";
	}

	/**
	 * Get the object for a booked PFN
	 *
	 * @param pfn
	 * @return the object, if exactly one entry exists, <code>null</code> if it was not booked
	 * @throws IOException
	 *             if any problem (more than one entry, invalid SE ...)
	 */
	public static PFN getBookedPFN(final String pfn) throws IOException {
		try (DBFunctions db = getDB()) {
			db.setReadOnly(true);
			db.setCursorType(ResultSet.TYPE_SCROLL_INSENSITIVE);

			if (!db.query("SELECT *, binary2string(guid) as guid_as_string FROM LFN_BOOKED WHERE pfn=?;", false, pfn))
				throw new IOException("Could not get the booked details for this pfn, query execution failed");

			final int count = db.count();

			if (count == 0)
				return null;

			if (count > 1)
				throw new IOException("More than one entry with this pfn: '" + pfn + "'");

			final SE se = SEUtils.getSE(db.gets("se"));

			if (se == null)
				throw new IOException("This SE doesn't exist: '" + db.gets(2) + "' for '" + pfn + "'");

			final GUID guid = GUIDUtils.getGUID(UUID.fromString(db.gets("guid_as_string")), true);

			if (!guid.exists()) {
				guid.size = db.getl("size");
				guid.md5 = StringFactory.get(db.gets("md5sum"));
				guid.owner = StringFactory.get(db.gets("owner"));
				guid.gowner = StringFactory.get(db.gets("gowner"));
				guid.perm = "755";
				guid.ctime = new Date();
				guid.expiretime = null;
				guid.type = 0;
				guid.aclId = -1;
			}

			final PFN retpfn = new PFN(guid, se);

			retpfn.pfn = pfn;

			return retpfn;
		}
	}
}
