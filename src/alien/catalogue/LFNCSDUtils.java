package alien.catalogue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.driver.core.ConsistencyLevel;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.Format;

/**
 * LFNCSD utilities
 *
 * @author mmmartin
 *
 */
public class LFNCSDUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(LFNCSDUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(LFNCSDUtils.class.getCanonicalName());

	/** Thread pool */
	static ThreadPoolExecutor tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1); // TODO Runtime.getRuntime().availableProcessors()

	static {
		tPool.setKeepAliveTime(1, TimeUnit.MINUTES);
		tPool.allowCoreThreadTimeOut(true);
	}

	/**
	 * Cassandra table suffix
	 */
	static public String append_table = "";
	/**
	 * Cassandra consistency
	 */
	static public ConsistencyLevel clevel = ConsistencyLevel.QUORUM;

	/**
	 * the "-s" flag of AliEn `find`
	 */
	public static final int FIND_NO_SORT = 1;

	/**
	 * the "-d" flag of AliEn `find`
	 */
	public static final int FIND_INCLUDE_DIRS = 2;

	/**
	 * the "-y" flag of AliEn `find`
	 */
	public static final int FIND_BIGGEST_VERSION = 4;

	/**
	 * @param command
	 * @param base
	 * @param pattern
	 * @param parameters
	 * @param metadata
	 * @param flags
	 * @return list of lfns that fit the patterns, if any
	 */
	public static Collection<LFN_CSD> recurseAndFilterLFNs(final String command, final String base, final String pattern, final String parameters, final String metadata, final int flags) {
		final Set<LFN_CSD> ret;
		final AtomicInteger counter_left = new AtomicInteger();

		if ((flags & LFNCSDUtils.FIND_NO_SORT) != 0)
			ret = new LinkedHashSet<>();
		else
			ret = new TreeSet<>();

		String path = base;
		String file_pattern = pattern;
		int index = 0;

		// Split the base into directories, change asterisk and ints. to regex format
		ArrayList<String> path_parts;
		if (!path.endsWith("/"))
			path += "*/";

		path = Format.replace(Format.replace(path, "*", ".*"), "?", ".");

		path_parts = new ArrayList<>(Arrays.asList(path.split("/")));

		if (file_pattern.contains("/")) {
			file_pattern = "*" + file_pattern;
			file_pattern = Format.replace(Format.replace(file_pattern, "*", ".*"), "?", ".?");
			String[] pattern_parts = file_pattern.split("/");
			file_pattern = pattern_parts[pattern_parts.length - 1];

			for (int i = 0; i < pattern_parts.length - 1; i++) {
				path_parts.add(pattern_parts[i]);
			}

		}
		else {
			file_pattern = Format.replace(Format.replace(file_pattern, "*", ".*"), "?", ".?");
		}

		path = "/";
		path_parts.remove(0); // position 0 otherwise is an empty string
		for (int i = 0; i < path_parts.size(); i++) {
			String s = path_parts.get(i);
			if (s.contains(".*") || s.contains(".?"))
				break;
			path += s + "/";
			index++;
		}

		logger.info("Going to recurseAndFilterLFNs: " + path + " - " + file_pattern + " - " + index + " - " + flags + " - " + path_parts.toString());

		counter_left.incrementAndGet();
		try {
			tPool.submit(new RecurseLFNs(ret, counter_left, path, file_pattern, index, path_parts, flags));
		} catch (RejectedExecutionException ree) {
			logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit: " + ree);
			return null;
		}

		while (counter_left.get() > 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't wait?: " + e);
			}
		}

		return ret;
	}

	private static class RecurseLFNs implements Runnable {
		final Collection<LFN_CSD> col;
		final AtomicInteger counter_left;
		final String base;
		final String file_pattern;
		final int index;
		final ArrayList<String> parts;
		final int flags;

		public RecurseLFNs(final Collection<LFN_CSD> col, final AtomicInteger counter_left, final String base, final String file_pattern, final int index, final ArrayList<String> parts,
				final int flags) {
			this.col = col;
			this.counter_left = counter_left;
			this.base = base;
			this.file_pattern = file_pattern;
			this.index = index;
			this.parts = parts;
			this.flags = flags;
		}

		@Override
		public String toString() {
			return base + index;
		}

		@Override
		public void run() {
			boolean lastpart = (index >= parts.size());

			boolean includeDirs = false;
			if ((flags & LFNCSDUtils.FIND_INCLUDE_DIRS) != 0)
				includeDirs = true;

			LFN_CSD dir = new LFN_CSD(base, true, append_table, null, null);
			if (!dir.exists || dir.type != 'd') {
				logger.severe("LFNCSDUtils recurseAndFilterLFNs: initial dir invalid - " + base);
				return;
			}

			List<LFN_CSD> list = dir.list(true, append_table, clevel);

			Pattern p;
			if (lastpart)
				p = Pattern.compile(file_pattern);
			else
				p = Pattern.compile(parts.get(index));

			// loop entries
			for (LFN_CSD lfnc : list) {
				if (lfnc.type != 'd') {
					// no dir
					if (lastpart) {
						// check pattern
						// Pattern p = Pattern.compile(file_pattern);
						Matcher m = p.matcher(lfnc.child);
						if (m.matches()) {
							System.out.println("Matches: " + lfnc.child);
							col.add(lfnc);
						}
					}
				}
				else {
					// dir
					if (lastpart) {
						// if we already passed the hierarchy introduced on the command, all dirs are valid
						try {
							if (includeDirs) {
								// Pattern p = Pattern.compile(file_pattern);
								Matcher m = p.matcher(lfnc.child);
								if (m.matches())
									col.add(lfnc);
							}
							counter_left.incrementAndGet();
							System.out.println("Submit(l): " + base + lfnc.child + "/");
							tPool.submit(new RecurseLFNs(col, counter_left, base + lfnc.child + "/", file_pattern, index + 1, parts, flags));
						} catch (RejectedExecutionException ree) {
							logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit dir(l) - " + base + lfnc.child + "/" + ": " + ree);
							counter_left.decrementAndGet();
						}
					}
					else {
						// while exploring introduced dir, need to check patterns
						// Pattern p = Pattern.compile(parts.get(index));
						Matcher m = p.matcher(lfnc.child);
						if (m.matches()) {
							// submit the dir
							try {
								counter_left.incrementAndGet();
								System.out.println("Submit: " + base + lfnc.child + "/");
								tPool.submit(new RecurseLFNs(col, counter_left, base + lfnc.child + "/", file_pattern, index + 1, parts, flags));
							} catch (RejectedExecutionException ree) {
								logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit dir - " + base + lfnc.child + "/" + ": " + ree);
								counter_left.decrementAndGet();
							}
						}
					}
				}

			}

			counter_left.decrementAndGet();
		}
	}

}
