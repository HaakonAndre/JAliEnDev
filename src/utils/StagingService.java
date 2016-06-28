package utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.protocols.Factory;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.DBFunctions;
import lazyj.Format;

/**
 * Staging daemon service. It checks the content of alice_users.staging_queue and executes the Xrootd prepare command for all replicas of the queued LFNs.
 *
 * @author costing
 * @since 2016-06-21
 */
public class StagingService {
	private static final LinkedBlockingQueue<Runnable> executorQueue = new LinkedBlockingQueue<>();
	private static final LinkedBlockingQueue<Runnable> bgexecutorQueue = new LinkedBlockingQueue<>();

	private static final int executorThreads = ConfigUtils.getConfig().geti("utils.StagingService.executorThreads", 16);
	private static final int bgexecutorThreads = ConfigUtils.getConfig().geti("utils.StagingService.bgexecutorThreads", 16);

	private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(executorThreads, executorThreads, 5, TimeUnit.SECONDS, executorQueue);
	static final ThreadPoolExecutor bgexecutor = new ThreadPoolExecutor(bgexecutorThreads, bgexecutorThreads, 5, TimeUnit.SECONDS, bgexecutorQueue);

	static final AtomicLong PREPARED_COMMANDS = new AtomicLong();

	static {
		executor.allowCoreThreadTimeOut(true);
	}

	static DBFunctions getDB() {
		return ConfigUtils.getDB("alice_users");
	}

	private static class StagePFN implements Runnable {
		final PFN pfn;

		public StagePFN(final PFN pfn) {
			this.pfn = pfn;
		}

		@Override
		public void run() {
			try {
				Factory.xrootd.prepare(pfn);
			} catch (final IOException ioe) {
				System.err.println("Could not background stage: " + pfn.getPFN() + ", the error message was:\n" + ioe.getMessage());
			}
		}
	}

	private static class StageLFN implements Runnable {

		final String lfn;

		public StageLFN(final DBFunctions db) {
			lfn = db.gets(1);
		}

		@Override
		public void run() {
			boolean delete = true;
			final LFN l = LFNUtils.getLFN(lfn);

			if (l != null) {
				final Set<PFN> pfns = l.whereis();

				if (pfns != null)
					for (final PFN p : pfns)
						if (syncSEs.size() == 0 || syncSEs.contains(Integer.valueOf(p.seNumber)))
							try {
								Factory.xrootd.prepareCond(p);
							} catch (final IOException ioe) {
								System.err.println("Could not stage: " + p.getPFN() + ", the error message was:\n" + ioe.getMessage());
								delete = false;
							}
						else
							bgexecutor.submit(new StagePFN(p));
			}

			try (DBFunctions db = getDB()) {
				if (delete) {
					PREPARED_COMMANDS.incrementAndGet();
					db.query("DELETE FROM staging_queue WHERE lfn=?;", false, lfn);
				}
				else
					db.query("UPDATE staging_queue SET attempts=attempts+1 WHERE lfn=?;", false, lfn);
			}
		}
	}

	final static Set<Integer> syncSEs = new HashSet<>();

	/**
	 * Service entry point
	 *
	 * @param args
	 *            SEs (number or full name) for which the staging is done synchronously. For other PFNs the request is queued to be tried, once, at a later time.
	 * @throws InterruptedException
	 */
	public static void main(final String[] args) throws InterruptedException {
		boolean lastNoWork = false;

		for (final String arg : args)
			try {
				syncSEs.add(Integer.valueOf(arg));
			} catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				final SE se = SEUtils.getSE(arg);

				if (se != null)
					syncSEs.add(Integer.valueOf(se.seNumber));
			}

		try (DBFunctions db = getDB()) {
			while (true) {
				db.query("DELETE FROM staging_queue WHERE attempts>10 OR created<adddate(now(), interval -1 month);");
				db.query("SELECT lfn FROM staging_queue ORDER BY attempts ASC, created ASC LIMIT 100000;");

				if (!db.moveNext()) {
					if (!lastNoWork)
						System.err.println("No work for me, hybernating for a while more");

					lastNoWork = true;
					Thread.sleep(1000 * 30);
					continue;
				}

				lastNoWork = false;

				do
					executor.submit(new StageLFN(db));
				while (db.moveNext());

				while (executorQueue.size() > 0) {
					final long lastValue = PREPARED_COMMANDS.get();
					final long lastTimestamp = System.currentTimeMillis();

					System.err.println("Queue is " + executorQueue.size() + " long, waiting for the current work queue to finish (bg executor queue: " + bgexecutorQueue.size() + ")");
					Thread.sleep(1000 * 30);
					System.err.println("  command rate in this batch: " + Format.point((PREPARED_COMMANDS.get() - lastValue) * 1000. / (System.currentTimeMillis() - lastTimestamp)) + " Hz");
				}
			}
		}
	}
}
