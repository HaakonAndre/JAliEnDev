package alien.io.xrootd;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.DBFunctions;
import lazyj.Format;

/**
 * Single threaded listing of Xrootd server content and injecting in the deletion queue the files that should not be there
 *
 * @author costing
 *
 */
public class XrootdCleanupSingle extends Thread {
	/**
	 * Storage element we are working on
	 */
	final SE se;

	private final String server;

	private final AtomicLong sizeRemoved = new AtomicLong();
	private final AtomicLong sizeKept = new AtomicLong();
	private final AtomicLong filesRemoved = new AtomicLong();
	private final AtomicLong filesKept = new AtomicLong();
	private final AtomicLong dirsSeen = new AtomicLong();

	/**
	 * How many items are currently in progress
	 */
	final AtomicInteger inProgress = new AtomicInteger(0);

	/**
	 * how many files were processed so far
	 */
	final AtomicInteger processed = new AtomicInteger(0);

	/**
	 * If <code>true</code> then pass the SE for dCache and DPM SEs
	 */
	final boolean setSE;

	private static DBFunctions getDB() {
		final DBFunctions db = ConfigUtils.getDB("alice_users");

		db.setQueryTimeout(600);

		return db;
	}

	/**
	 * Check all GUID files in this storage by listing recursively its contents.
	 *
	 * @param sSE
	 */
	public XrootdCleanupSingle(final String sSE) {
		se = SEUtils.getSE(sSE);

		if (se == null) {
			server = null;
			setSE = false;

			System.err.println("No such SE " + sSE);

			return;
		}

		try (DBFunctions db = getDB()) {
			db.query("CREATE TABLE IF NOT EXISTS orphan_pfns_" + se.seNumber + " LIKE orphan_pfns_0;", true);
		}

		setSE = se.getName().toLowerCase().contains("dcache") || se.getName().toLowerCase().contains("dpm");

		String sBase = se.seioDaemons;

		if (sBase.startsWith("root://"))
			sBase = sBase.substring(7);

		server = sBase;
	}

	/**
	 * @param path
	 */
	void storageCleanup(final String path) {
		final String actualPath = Format.replace(SE.generateProtocol(se.seStoragePath, path), "//", "/");

		dirsSeen.incrementAndGet();

		try {
			final XrootdListing listing = new XrootdListing(server, actualPath, setSE ? se : null);

			for (final XrootdFile file : listing.getFiles())
				fileCheck(file);

			for (final XrootdFile dir : listing.getDirs()) {
				final int idx = dir.path.indexOf(actualPath);

				if (idx >= 0 && dir.path.matches(".*/\\d{2}(/\\d{5})?/?$"))
					storageCleanup(dir.path.substring(idx + actualPath.length() - path.length()));
			}
		} catch (final IOException ioe) {
			System.err.println(ioe.getMessage());
			ioe.printStackTrace();
		}
	}

	private void fileCheck(final XrootdFile file) {
		try {
			if (System.currentTimeMillis() - file.date.getTime() < 1000 * 60 * 60 * 24)
				// ignore very recent files
				return;

			final UUID uuid;

			try {
				uuid = UUID.fromString(file.getName());
			} catch (@SuppressWarnings("unused") final Exception e) {
				// not an alien file name, ignore
				return;
			}

			final GUID guid = GUIDUtils.getGUID(uuid);

			boolean remove = false;

			if (guid == null)
				remove = true;
			else {
				final Set<PFN> pfns = guid.getPFNs();

				if (pfns == null || pfns.size() == 0)
					remove = true;
				else {
					boolean found = false;

					for (final PFN pfn : pfns)
						if (se.equals(pfn.getSE())) {
							found = true;
							break;
						}

					remove = !found;
				}
			}

			if (remove) {
				if (removeFile(file)) {
					sizeRemoved.addAndGet(file.size);
					filesRemoved.incrementAndGet();
				}
			}
			else {
				sizeKept.addAndGet(file.size);
				filesKept.incrementAndGet();
			}
		} catch (final Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}

		processed.incrementAndGet();
	}

	// B6B6EF58-4000-11E0-9CE5-001F29EB8B98
	private static final Pattern UUID_PATTERN = Pattern.compile(".*([0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}).*");

	private boolean removeFile(final XrootdFile file) {
		final Matcher m = UUID_PATTERN.matcher(file.getName());

		final UUID uuid;

		final String sUUID;

		if (m.matches()) {
			sUUID = m.group(1);
			uuid = UUID.fromString(sUUID);
		}
		else
			return false;

		System.err.println("RM " + uuid + " FROM " + se.seName + ", " + file.size + " (" + Format.size(file.size) + "), " + file.date);

		try (DBFunctions db = getDB()) {
			db.setQueryTimeout(600);
			if (sUUID.equals(uuid.toString()))
				db.query("INSERT IGNORE INTO orphan_pfns_" + se.seNumber + " (flags,guid,se,size) VALUES (1,string2binary(?), ?, ?);", false, uuid.toString(), Integer.valueOf(se.seNumber),
						Long.valueOf(file.size));
			else
				db.query("INSERT IGNORE INTO orphan_pfns_" + se.seNumber + " (flags,guid,se,size,pfn) VALUES (1,string2binary(?), ?, ?, ?);", false, uuid.toString(), Integer.valueOf(se.seNumber),
						Long.valueOf(file.size), SE.generateProtocol(se.seioDaemons, file.path));
		}

		return true;
	}

	@Override
	public String toString() {
		return "Removed " + filesRemoved + " files (" + Format.size(sizeRemoved.longValue()) + "), " + "kept " + filesKept + " files (" + Format.size(sizeKept.longValue()) + "), listed " + dirsSeen
				+ " directories from " + se.seName;
	}

	@Override
	public void run() {
		final long lStart = System.currentTimeMillis();

		for (int i = 0; i <= 15; i++) {
			storageCleanup((i < 10 ? "0" : "") + i + "/");

			System.err.println("Progress report (" + i + "): " + toString() + ", took " + Format.toInterval(System.currentTimeMillis() - lStart));
		}

		System.err.println("Final report: " + toString() + ", took " + Format.toInterval(System.currentTimeMillis() - lStart));
	}

	/**
	 * @param args
	 *            the only argument taken by this class is the name of the storage to be cleaned
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws IOException, InterruptedException {
		final OptionParser parser = new OptionParser();

		parser.accepts("?", "Print this help");
		parser.accepts("a", "Run on all known SEs");
		parser.accepts("d", "Run on all known disk-only SEs");

		final OptionSet options = parser.parse(args);

		if ((options.nonOptionArguments().size() == 0 && !options.has("a") && !options.has("d")) || options.has("?")) {
			parser.printHelpOn(System.out);
			return;
		}

		final Collection<String> ses = new LinkedList<>();

		if (options.has("a"))
			for (final SE se : SEUtils.getSEs(null))
				ses.add(se.getName());
		else
			if (options.has("d")) {
				for (final SE se : SEUtils.getSEs(null))
					if (se.isQosType("disk"))
						ses.add(se.getName());
			}
			else
				for (final Object o : options.nonOptionArguments())
					ses.add(o.toString());

		final Map<String, XrootdCleanupSingle> progress = new TreeMap<>();

		for (final String se : ses) {
			final XrootdCleanupSingle cleanup = new XrootdCleanupSingle(se);

			cleanup.start();

			progress.put(se.toUpperCase(), cleanup);
		}

		boolean active = true;

		do {
			Thread.sleep(1000 * 60);

			active = false;

			long totalSizeRemoved = 0;
			long totalSizeKept = 0;
			long totalFilesRemoved = 0;
			long totalFilesKept = 0;
			long totalDirsSeen = 0;

			try (FileWriter fw = new FileWriter("XrootdCleanupSingle.progress")) {
				for (final Map.Entry<String, XrootdCleanupSingle> entry : progress.entrySet()) {
					final XrootdCleanupSingle cleanup = entry.getValue();

					active = active || cleanup.isAlive();

					fw.write(entry.getKey() + " : " + (active ? "RUNNING" : "DONE") + " " + cleanup + "\n");

					totalSizeRemoved += cleanup.sizeRemoved.longValue();
					totalSizeKept += cleanup.sizeKept.longValue();
					totalFilesRemoved += cleanup.filesRemoved.longValue();
					totalFilesKept += cleanup.filesKept.longValue();
					totalDirsSeen += cleanup.dirsSeen.longValue();
				}

				fw.write("Overall progress: " + totalFilesRemoved + " files (" + Format.size(totalSizeRemoved) + " removed / " + totalFilesKept + " files (" + Format.size(totalSizeKept) + " kept, "
						+ totalDirsSeen + " directories visited");
			} catch (final IOException ioe) {
				System.err.println("Cannot dump stats, error was: " + ioe.getMessage());
			}
		} while (active);

		System.err.println("Work finished, see you next time");
	}
}
