package alien.catalogue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lia.util.process.ExternalProcesses;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.AliEnPrincipal;

/**
 * @author costing
 * 
 */
public final class GUIDUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(GUIDUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(GUIDUtils.class.getCanonicalName());

	/**
	 * Get the host where this entry should be located
	 * 
	 * @param guid
	 * @return host id
	 * @see Host
	 */
	public static int getGUIDHost(final UUID guid) {
		final long guidTime = indexTime(guid);

		final GUIDIndex index = CatalogueUtils.getGUIDIndex(guidTime);

		if (index == null)
			return -1;

		return index.hostIndex;
	}

	/**
	 * Get the DB connection that applies for a particular GUID
	 * 
	 * @param guid
	 * @return the DB connection, or <code>null</code> if something is not right
	 * @see #getTableNameForGUID(UUID)
	 */
	public static DBFunctions getDBForGUID(final UUID guid) {
		final int host = getGUIDHost(guid);

		if (host < 0)
			return null;

		final Host h = CatalogueUtils.getHost(host);

		if (h == null)
			return null;

		return h.getDB();
	}

	/**
	 * Get the tablename where this GUID should be located (if any)
	 * 
	 * @param guid
	 * @return table name, or <code>null</code> if any problem
	 * @see #getDBForGUID(UUID)
	 */
	public static int getTableNameForGUID(final UUID guid) {
		final long guidTime = indexTime(guid);

		final GUIDIndex index = CatalogueUtils.getGUIDIndex(guidTime);

		if (index == null)
			return -1;

		return index.tableName;
	}

	/**
	 * @param l
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final LFN l) {
		return getGUID(l, false);
	}

	/**
	 * @param l
	 * @param evenIfDoesntExist
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final LFN l, final boolean evenIfDoesntExist) {
		final GUID g = getGUID(l.guid, evenIfDoesntExist);

		if (g == null)
			return null;

		g.addKnownLFN(l);

		return g;
	}

	/**
	 * Get the GUID catalogue entry when the uuid is known
	 * 
	 * @param uuid
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final String uuid) {
		return getGUID(UUID.fromString(uuid));
	}

	/**
	 * Get the GUID catalogue entry when the uuid is known
	 * 
	 * @param guid
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final UUID guid) {
		return getGUID(guid, false);
	}

	/**
	 * Get the referring GUIDs (members of the archive, if any)
	 * 
	 * @param guid
	 * @return the set of GUIDs pointing to this archive, or <code>null</code> if there is no such file
	 */
	public static Set<GUID> getReferringGUID(final UUID guid) {
		final int host = getGUIDHost(guid);

		if (host < 0)
			return null;

		final Host h = CatalogueUtils.getHost(host);

		if (h == null)
			return null;

		try (DBFunctions db = h.getDB()) {
			if (db == null)
				return null;

			db.setReadOnly(true);

			final int tableName = GUIDUtils.getTableNameForGUID(guid);

			if (tableName < 0)
				return null;

			if (monitor != null)
				monitor.incrementCounter("GUID_db_lookup");

			if (!db.query("select G" + tableName + "L.* from G" + tableName + "L INNER JOIN G" + tableName + "L_PFN USING (guidId) where pfn like ?;", false, "guid:///" + guid.toString() + "?ZIP=%"))
				throw new IllegalStateException("Failed querying the G" + tableName + "L table for guid " + guid);

			if (!db.moveNext())
				return null;

			final Set<GUID> ret = new TreeSet<>();

			do
				try {
					ret.add(new GUID(db, host, tableName));
				} catch (final Exception e) {
					logger.log(Level.WARNING, "Exception instantiating guid " + guid + " from " + tableName, e);

					return null;
				}
			while (db.moveNext());

			return ret;
		}
	}

	/**
	 * Get the GUID catalogue entry when the uuid is known
	 * 
	 * @param guid
	 * @param evenIfDoesntExist
	 *            if <code>true</code>, if the entry doesn't exist then a new GUID is returned
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final UUID guid, final boolean evenIfDoesntExist) {
		final int host = getGUIDHost(guid);

		if (host < 0)
			return null;

		final Host h = CatalogueUtils.getHost(host);

		if (h == null)
			return null;

		try (DBFunctions db = h.getDB()) {
			if (db == null)
				return null;

			final int tableName = GUIDUtils.getTableNameForGUID(guid);

			if (tableName < 0)
				return null;

			if (monitor != null)
				monitor.incrementCounter("GUID_db_lookup");

			db.setReadOnly(true);

			if (!db.query("SELECT * FROM G" + tableName + "L WHERE guid=string2binary(?);", false, guid.toString()))
				throw new IllegalStateException("Failed querying the G" + tableName + "L table for guid " + guid);

			if (!db.moveNext()) {
				if (evenIfDoesntExist)
					return new GUID(guid);

				return null;
			}

			try {
				return new GUID(db, host, tableName);
			} catch (final Exception e) {
				logger.log(Level.WARNING, "Exception instantiating guid " + guid + " from " + tableName, e);

				return null;
			}
		}
	}

	/**
	 * 
	 * check if the string contains a valid GUID
	 * 
	 * @param guid
	 * @return yesORno
	 */
	public static boolean isValidGUID(final String guid) {
		try {
			UUID.fromString(guid);
			return true;
		} catch (final Exception e) {
			return false;
		}
	}

	private static int clockSequence = MonitorFactory.getSelfProcessID();

	private static long lastTimestamp = System.nanoTime() / 100 + 122192928000000000L;

	private static long lastTimestamp2 = System.nanoTime() / 100 + 122192928000000000L;

	/**
	 * @return a time UUID with the reference time set to now
	 */
	public static synchronized UUID generateTimeUUID() {
		final long time = System.currentTimeMillis() * 10000 + 122192928000000000L;

		if (time <= lastTimestamp) {
			clockSequence++;

			if (clockSequence >= 65535)
				clockSequence = 0;
		}

		lastTimestamp = time;

		return generateTimeUUIDWork(time);
	}

	/**
	 * @param referenceTime
	 * @return a time UUID with the time field set to the reference time
	 */
	public static synchronized UUID generateTimeUUID(final long referenceTime) {
		final long time = referenceTime * 10000 + 122192928000000000L;

		if (time <= lastTimestamp2 || time <= lastTimestamp) {
			clockSequence++;

			if (clockSequence >= 65535)
				clockSequence = 0;
		}

		lastTimestamp2 = time;

		return generateTimeUUIDWork(time);
	}

	/**
	 * @return a new time-based (version 1) UUID
	 */
	private static UUID generateTimeUUIDWork(final long time) {
		final byte[] contents = new byte[16];

		final byte[] mac = getMac();

		for (int i = 0; i < 6; i++)
			contents[10 + i] = mac[i];

		final int timeHi = (int) (time >>> 32);
		final int timeLo = (int) time;

		contents[0] = (byte) (timeLo >>> 24);
		contents[1] = (byte) (timeLo >>> 16);
		contents[2] = (byte) (timeLo >>> 8);
		contents[3] = (byte) (timeLo);

		contents[4] = (byte) (timeHi >>> 8);
		contents[5] = (byte) timeHi;
		contents[6] = (byte) (timeHi >>> 24);
		contents[7] = (byte) (timeHi >>> 16);

		contents[8] = (byte) (clockSequence >> 8);
		contents[9] = (byte) clockSequence;

		contents[6] &= (byte) 0x0F;
		contents[6] |= (byte) 0x10;

		contents[8] &= (byte) 0x3F;
		contents[8] |= (byte) 0x80;

		final UUID ret = GUID.getUUID(contents);

		return ret;
	}

	/**
	 * Extract the MAC address from the given UUID. There is no guarantee on the value of this field, it's just the bytes that would have the MAC address in a v1 UUID.
	 * 
	 * @param uuid
	 * @return
	 */
	public static String getMacAddr(final UUID uuid) {
		long mac = uuid.getLeastSignificantBits() & 0xFFFFFFFFFFFFL;

		return String.format("%02x:%02x:%02x:%02x:%02x:%02x", Long.valueOf(mac >> 40 & 0xFF), Long.valueOf(mac >> 32 & 0xFF), Long.valueOf(mac >> 24 & 0xFF), Long.valueOf(mac >> 16 & 0xFF),
				Long.valueOf(mac >> 8 & 0xFF), Long.valueOf(mac & 0xFF));
	}

	private static byte[] MACAddress = null;

	private static final String SYS_ENTRY = "/sys/class/net";

	private static synchronized byte[] getMac() {
		if (MACAddress == null) {
			// figure it out
			MACAddress = new byte[6];

			String sMac = null;

			final File f = new File(SYS_ENTRY);

			if (f.exists()) {
				final String[] devices = f.list();

				if (devices != null) {
					final List<String> files = Arrays.asList(devices);

					Collections.sort(files);

					for (final String dev : devices) {
						final String addr = lazyj.Utils.readFile(SYS_ENTRY + "/" + dev + "/address");

						if (addr != null && !addr.equals("00:00:00:00:00:00")) {
							sMac = addr;
							break;
						}
					}
				}
			}

			if (sMac == null)
				try (BufferedReader br = new BufferedReader(new StringReader(ExternalProcesses.getCmdOutput(Arrays.asList("/sbin/ifconfig", "-a"), false, 30, TimeUnit.SECONDS)))) {
					String s;

					while ((s = br.readLine()) != null) {
						final StringTokenizer st = new StringTokenizer(s);

						while (st.hasMoreTokens()) {
							final String tok = st.nextToken();

							if (tok.equals("HWaddr") && st.hasMoreTokens()) {
								sMac = st.nextToken();
								break;
							}
						}

						if (sMac != null)
							break;
					}
				} catch (final Throwable t) {
					// ignore
				}

			if (sMac != null) {
				final StringTokenizer st = new StringTokenizer(sMac.trim(), ":");

				for (int i = 0; i < 6; i++) {
					try {
						MACAddress[i] = (byte) Integer.parseInt(st.nextToken(), 16);
					} catch (final NumberFormatException nfe) {
						// ignore
					}
				}
			}
		}

		return MACAddress;
	}

	/**
	 * @return a new (empty) GUID
	 */
	public static GUID createGuid() {
		UUID id;
		// do{
		// id = generateTimeUUID();
		// } while (getGUID(id) != null);

		id = generateTimeUUID();

		return new GUID(id);
	}

	/**
	 * @param user
	 * @return a new GUID
	 */
	public static GUID createGuid(final AliEnPrincipal user) {
		final GUID guid = createGuid();

		if (user != null) {
			guid.owner = user.getName();

			final Set<String> roles = user.getRoles();

			if (roles != null && roles.size() > 0)
				guid.gowner = roles.iterator().next();
			else
				guid.gowner = guid.owner;
		}

		guid.type = 0; // as in the catalogue
		guid.perm = "755";
		guid.aclId = -1;

		return guid;
	}

	/**
	 * @param f
	 *            base file to fill the properties from: ctime, md5, sizeSystem.
	 * @param user
	 *            who owns this new entry
	 * @return the newly created GUID
	 * @throws IOException
	 */
	public static GUID createGuid(final File f, final AliEnPrincipal user) throws IOException {
		final String md5 = IOUtils.getMD5(f);

		final GUID guid = createGuid(user);

		guid.ctime = new Date(f.lastModified());
		guid.md5 = md5;
		guid.size = f.length();

		return guid;
	}

	/**
	 * @param uuid
	 * @return epoch time of this uuid
	 */
	public static final long epochTime(final UUID uuid) {
		return (uuid.timestamp() - 0x01b21dd213814000L) / 10000;
	}

	/**
	 * @param uuid
	 * @return AliEn guidtime-compatible value
	 */
	public static final long indexTime(final UUID uuid) {
		final long msg = uuid.getMostSignificantBits() & 0x00000000FFFFFFFFL;

		long ret = (msg >>> 16);
		ret += (msg & 0x0FFFFL) << 16;

		return ret;
	}

	/**
	 * @param uuid
	 * @return index time as string
	 */
	public static final String getIndexTime(final UUID uuid) {
		return Long.toHexString(indexTime(uuid)).toUpperCase();
	}

	/**
	 * Check if the MD5 sum is set to both the LFN and the underlying GUID. If not set the missing one (or both) from the other or by downloading the file and computing the MD5 sum.
	 * 
	 * @param lfn
	 * @return <code>true</code> if the MD5 was already set or if it could be now set, <code>false</code> if there was any error setting it
	 */
	public static boolean checkMD5(final LFN lfn) {
		final GUID g = getGUID(lfn);

		if (g == null) {
			logger.log(Level.WARNING, "No GUID for " + lfn.getCanonicalName());
			return false;
		}

		if (lfn.md5 == null || lfn.md5.length() < 10 || g.md5 == null || g.md5.length() < 10) {
			if (g.md5 != null && g.md5.length() >= 10) {
				lfn.md5 = g.md5;
				logger.log(Level.INFO, "Setting md5 of " + g.guid + " from " + lfn.getCanonicalName() + " to " + lfn.md5);
				return lfn.update();
			}

			if (lfn.md5 != null && lfn.md5.length() >= 10) {
				g.md5 = lfn.md5;
				logger.log(Level.INFO, "Setting md5 of " + lfn.getCanonicalName() + " from " + g.guid + " to " + g.md5);
				return g.update();
			}

			final String reason = AuthorizationFactory.fillAccess(g, AccessType.READ);

			if (reason != null) {
				logger.log(Level.WARNING, "Could not get authorization to read " + g.guid + " : " + reason);
				return false;
			}

			File temp = IOUtils.get(g);

			if (temp != null) {
				try {
					g.md5 = IOUtils.getMD5(temp);

					if (!g.update())
						return false;

					lfn.md5 = g.md5;

					if (!lfn.update())
						return false;

					return true;
				} catch (final IOException ioe) {
					logger.log(Level.WARNING, "Unable to compute the MD5 sum of " + lfn.getCanonicalName(), ioe);

					return false;
				} finally {
					temp.delete();
				}
			}
			logger.log(Level.WARNING, "Could not download " + g.guid);

			return false;
		}

		return true;
	}
}
