package alien.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;

/**
 *
 */
public class CatalogueToCatalogueThreads {
	/** Array of thread-dir */
	static final HashMap<Long, LFN> activeThreadFolders = new HashMap<>();

	/** Thread pool */
	static ThreadPoolExecutor tPool = null;

	static boolean shouldexit = false;

	/**
	 * limit
	 */
	static final int origlimit = 2000000;
	/** Entries processed */
	static AtomicInteger global_count = new AtomicInteger();
	/**
	 * Limit number of entries
	 */
	static AtomicInteger limit = new AtomicInteger(origlimit);

	/**
	 * Limit number of entries
	 */
	static AtomicInteger timing_count = new AtomicInteger();

	/**
	 * total milliseconds
	 */
	static AtomicLong ns_count = new AtomicLong();

	/** File for tracking created folders */
	static PrintWriter out = null;
	/**
	 * Various log files
	 */
	static PrintWriter pw = null;
	/**
	 * Log file
	 */
	static PrintWriter failed_folders = null;
	/**
	 * Log file
	 */
	static PrintWriter failed_files = null;
	/**
	 * Log file
	 */
	static PrintWriter failed_collections = null;
	/**
	 * Log files
	 */
	static PrintWriter failed_ses = null;
	/**
	 * Log file
	 */
	static PrintWriter used_threads = null;
	/**
	 * Suffix for log files
	 */
	static String logs_suffix = "";

	static Random rdm = new Random();

	static final SE se1 = SEUtils.getSE(332);

	static final SE se2 = SEUtils.getSE(320);

	public static String getmd5(String str) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(str.getBytes());
			byte[] digest = md.digest();
			StringBuffer sb = new StringBuffer();
			for (byte b : digest) {
				sb.append(String.format("%02x", b & 0xff));
			}
			//
			// System.out.println("original:" + original);
			return sb.toString();
		} catch (Exception e) {
			System.err.println("Exception generating md5: " + e);
		}
		return null;
	}

	/**
	 * auto-generated paths
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		final int nargs = args.length;

		if (nargs < 1) {
			System.err.println("Usage: ./run.sh alien/src/test/CatalogueToCassandraThreads <...>");
			System.err.println("E.g. <base> -> 0");
			System.err.println("E.g. <limit> -> 1000 (it creates 1000*10)");
			System.err.println("E.g. <pool_size> -> 12");
			System.err.println("E.g. <logs-suffix> -> alice-md5-1M");
			System.exit(-3);
		}

		final Long base = Long.parseLong(args[0]);
		final Long limit = Long.parseLong(args[1]);
		// final long until = limit + base;

		int pool_size = 16;
		if (nargs > 3)
			pool_size = Integer.parseInt(args[2]);
		System.out.println("Pool size: " + pool_size);
		tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(pool_size);
		tPool.setKeepAliveTime(1, TimeUnit.MINUTES);
		tPool.allowCoreThreadTimeOut(true);

		if (nargs > 4)
			logs_suffix = "-" + args[3];

		System.out.println("Printing output to: out" + logs_suffix);
		out = new PrintWriter(new FileOutputStream("out" + logs_suffix));
		out.println("Starting: " + new Date());
		out.flush();

		System.out.println("Printing threads to used_threads" + logs_suffix);
		used_threads = new PrintWriter(new FileOutputStream("used_threads" + logs_suffix));
		used_threads.println(logs_suffix + " - " + pool_size);
		used_threads.close();

		System.out.println("Going to insert " + limit + "*10 in hierarchy. Time: " + new Date());

		// Create LFN paths and submit them
		for (Long i = base; i < limit; i++) {
			tPool.submit(new AddPath(i));
		}

		try {
			while (!tPool.awaitTermination(20, TimeUnit.SECONDS)) {
				final int tCount = tPool.getActiveCount();
				final int qSize = tPool.getQueue().size();
				System.out.println("Awaiting completion of threads..." + tCount + " - " + qSize);
				if (tCount == 0 && qSize == 0) {
					tPool.shutdown();
					System.out.println("Shutdown executor");
				}
			}
		} catch (final InterruptedException e) {
			System.err.println("Something went wrong!: " + e);
		}

		double ms_per_i = 0;
		int cnt = timing_count.get();

		if (cnt > 0) {
			ms_per_i = ns_count.get() / (double) cnt;
			System.out.println("Final ns/i: " + ms_per_i);
			ms_per_i = ms_per_i / 1000000.;
		}
		else
			System.out.println("!!!!! Zero timing count !!!!!");

		System.out.println("Final timing count: " + cnt);
		System.out.println("Final ms/i: " + ms_per_i);

		out.println("Final timing count: " + cnt + " - " + new Date());
		out.println("Final ms/i: " + ms_per_i);
		out.close();
	}

	private static class AddPath implements Runnable {
		final Long root;
		// String path;

		public AddPath(final Long r) {
			this.root = r;
		}

		@Override
		public void run() {
			// Divide the md5 in 8 letter segments, first 3 directories, last
			// one filename
			// this.path = getmd5(root.toString());

			// String[] strs = this.path.split("(?<=\\G.{8})");
			// String lfn = "/cassandra/" + strs[0] + "/" + strs[1] + "/" +
			// strs[2] + "/" + strs[3];
			// String lfnparent = "/cassandra/" + strs[0] + "/" + strs[1] + "/"
			// + strs[2] + "/";

			long last_part = root % 10000;
			long left = root / 10000;
			long medium_part = left % 100;
			long first_part = left / 100;
			String lfnparent = "/cassandra/" + first_part + "/" + medium_part + "/" + last_part + "/";

			for (int i = 1; i <= 10; i++) {
				String lfn = lfnparent + "file" + i + "_" + root;
				// System.out.println("Processing: " + lfn);
				// if (true)
				// continue;

				int counted = global_count.incrementAndGet();
				if (counted % 5000 == 0) {
					//out.println("LFN: " + lfn + " - Count: " + counted + " Time: " + new Date());
					out.println("LFN: " + lfn + "Estimation: " + (ns_count.get() / counted) / 1000000. + 
							" - Count: " + counted + " Time: " + new Date());
					out.flush();
				}

				// Create LFN and GUID
				LFN lfnc = LFNUtils.getLFN(lfn, true);
				GUID guid = GUIDUtils.createGuid();

				lfnc.size = rdm.nextInt(100000);
				lfnc.guid = guid.guid;
				lfnc.jobid = (long) rdm.nextInt(1000000);
				lfnc.md5 = "ee31e454013aa515f0bc806aa907ba51";
				lfnc.type = 'f';
				lfnc.perm = "755";
				lfnc.ctime = new Date();
				lfnc.owner = "aliprod";
				lfnc.gowner = "aliprod";
				// lfnc.dir = rdm.nextInt(1000000);

				guid.ctime = lfnc.ctime;
				guid.gowner = "aliprod";
				guid.owner = "aliprod";
				guid.size = lfnc.size;
				guid.md5 = lfnc.md5;
				guid.perm = lfnc.perm;

				final long start = System.nanoTime();
				// Insert LFN and GUID
				if (!LFNUtils.insertLFN(lfnc)) { // changed visibility to public
					final String msg = "Error inserting lfn: " + lfnc.getCanonicalName() + " Time: " + new Date();
					System.err.println(msg);
					continue;
				}

				// Add PFNS change constructor to guid, se using 2 ses always
				PFN pfn1 = new PFN(guid, se1); // New constructor
				if (!guid.addPFN(pfn1)) {
					final String msg = "Error inserting pfn1: " + lfnc.getCanonicalName() + " Time: " + new Date();
					System.err.println(msg);
					continue;
				}

				PFN pfn2 = new PFN(guid, se2);
				if (!guid.addPFN(pfn2)) {
					final String msg = "Error inserting pfn2: " + lfnc.getCanonicalName() + " Time: " + new Date();
					System.err.println(msg);
					continue;
				}

				final long duration_ns = (long) ((System.nanoTime() - start));
				ns_count.addAndGet(duration_ns);
				timing_count.incrementAndGet();
			}
		}
	}

}
