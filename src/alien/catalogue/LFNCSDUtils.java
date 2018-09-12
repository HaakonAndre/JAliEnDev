package alien.catalogue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.nfunk.jep.JEP;

import com.datastax.driver.core.ConsistencyLevel;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
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
	static ThreadPoolExecutor tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

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
	 * @param start_path
	 * @param pattern
	 * @param metadata
	 * @param flags
	 * @return list of lfns that fit the patterns, if any
	 */
	public static Collection<LFN_CSD> recurseAndFilterLFNs(final String command, final String start_path, final String pattern, final String metadata, final int flags) {
		final Set<LFN_CSD> ret;
		final AtomicInteger counter_left = new AtomicInteger();

		// we create a base for search and a file pattern
		int index = 0;
		String path = start_path;
		String file_pattern = (pattern == null ? "*" : pattern);
		if (!start_path.endsWith("/") && pattern == null) {
			file_pattern = start_path.substring(start_path.lastIndexOf('/') + 1);
			path = start_path.substring(0, start_path.lastIndexOf('/') + 1);
		}

		// choose to use sorted/unsorted type according to flag (-s)
		if ((flags & LFNCSDUtils.FIND_NO_SORT) != 0)
			ret = new LinkedHashSet<>();
		else
			ret = new TreeSet<>();

		// Split the base into directories, change asterisk and interrogation marks to regex format
		ArrayList<String> path_parts;
		if (!path.endsWith("/"))
			path += "*/";

		path = Format.replace(Format.replace(path, "*", ".*"), "?", ".");

		path_parts = new ArrayList<>(Arrays.asList(path.split("/")));

		if (file_pattern.contains("/")) {
			file_pattern = "*" + file_pattern;
			file_pattern = Format.replace(Format.replace(file_pattern, "*", ".*"), "?", ".?");
			// String[] pattern_parts = file_pattern.split("/");
			// file_pattern = pattern_parts[pattern_parts.length - 1];

			// for (int i = 0; i < pattern_parts.length - 1; i++) {
			// path_parts.add(pattern_parts[i]);
			// }
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

		Pattern pat = Pattern.compile(file_pattern);

		logger.info("Going to recurseAndFilterLFNs: " + path + " - " + file_pattern + " - " + index + " - " + flags + " - " + path_parts.toString());

		counter_left.incrementAndGet();
		try {
			tPool.submit(new RecurseLFNs(ret, counter_left, path, pat, index, path_parts, flags, metadata, (command.equals("find") ? true : false)));
		} catch (RejectedExecutionException ree) {
			logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit: " + ree);
			return null;
		}

		while (counter_left.get() > 0) {
			try {
				Thread.sleep(200);
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
		final Pattern file_pattern;
		final int index;
		final ArrayList<String> parts;
		final int flags;
		final String metadata;
		final boolean recurseInfinite;

		public RecurseLFNs(final Collection<LFN_CSD> col, final AtomicInteger counter_left, final String base, final Pattern file_pattern, final int index, final ArrayList<String> parts,
				final int flags, final String metadata, final boolean recurse) {
			this.col = col;
			this.counter_left = counter_left;
			this.base = base;
			this.file_pattern = file_pattern;
			this.index = index;
			this.parts = parts;
			this.flags = flags;
			this.metadata = metadata;
			this.recurseInfinite = recurse;
		}

		@Override
		public String toString() {
			return base + index;
		}

		@Override
		public void run() {
			boolean lastpart = (!recurseInfinite && index >= parts.size());

			boolean includeDirs = false;
			if ((flags & LFNCSDUtils.FIND_INCLUDE_DIRS) != 0)
				includeDirs = true;

			LFN_CSD dir = new LFN_CSD(base, true, append_table, null, null);
			if (!dir.exists || dir.type != 'd') {
				logger.severe("LFNCSDUtils recurseAndFilterLFNs: initial dir invalid - " + base);
				counter_left.decrementAndGet();
				return;
			}

			List<LFN_CSD> list = dir.list(true, append_table, clevel);

			Pattern p;
			if (lastpart || recurseInfinite)
				p = this.file_pattern;
			else
				p = Pattern.compile(parts.get(index));

			JEP jep = null;

			if (metadata != null && !metadata.equals("")) {
				jep = new JEP();
				jep.setAllowUndeclared(true);
				String expression = Format.replace(Format.replace(Format.replace(metadata, "and", "&&"), "or", "||"), ":", "__");
				jep.parseExpression(expression);
			}

			ArrayList<LFN_CSD> filesVersion = null;
			if ((flags & LFNCSDUtils.FIND_BIGGEST_VERSION) != 0)
				filesVersion = new ArrayList<>();

			// loop entries
			for (LFN_CSD lfnc : list) {
				if (lfnc.type != 'd') {
					// no dir
					if (lastpart || recurseInfinite) {
						// check pattern
						Matcher m = p.matcher(recurseInfinite ? lfnc.canonicalName : lfnc.child);
						if (m.matches()) {
							if (jep != null) {
								// we check the metadata of the file against our expression
								Set<String> keys_values = new HashSet<>();

								// set the variable values from the metadata map
								for (String s : lfnc.metadata.keySet()) {
									Double value;
									try {
										value = Double.valueOf(lfnc.metadata.get(s));
									} catch (NumberFormatException e) {
										logger.info("Skipped: " + s + e);
										continue;
									}
									keys_values.add(s);
									jep.addVariable(s, value);
								}

								try {
									// This should return 1.0 or 0.0
									Object result = jep.getValueAsObject();
									if (result != null && result instanceof Double && ((Double) result).intValue() == 1.0) {
										if (filesVersion != null)
											filesVersion.add(lfnc);
										else
											col.add(lfnc);
									}
								} catch (Exception e) {
									logger.info("RecurseLFNs metadata - cannot get result: " + e);
								}

								// unset the variables for the next lfnc to be processed
								for (String s : keys_values) {
									jep.setVarValue(s, null);
								}
							}
							else {
								if (filesVersion != null)
									filesVersion.add(lfnc);
								else
									col.add(lfnc);
							}
						}
					}
				}
				else {
					// dir
					if (lastpart || recurseInfinite) {
						// if we already passed the hierarchy introduced on the command, all dirs are valid
						try {
							if (includeDirs) {
								Matcher m = p.matcher(recurseInfinite ? lfnc.canonicalName : lfnc.child);
								if (m.matches())
									col.add(lfnc);
							}
							if (recurseInfinite) {
								counter_left.incrementAndGet();
								tPool.submit(new RecurseLFNs(col, counter_left, base + lfnc.child + "/", file_pattern, index + 1, parts, flags, metadata, recurseInfinite));
							}
						} catch (RejectedExecutionException ree) {
							logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit dir(l) - " + base + lfnc.child + "/" + ": " + ree);
						}
					}
					else {
						// while exploring introduced dir, need to check patterns
						Matcher m = p.matcher(lfnc.child);
						if (m.matches()) {
							// submit the dir
							try {
								counter_left.incrementAndGet();
								tPool.submit(new RecurseLFNs(col, counter_left, base + lfnc.child + "/", file_pattern, index + 1, parts, flags, metadata, recurseInfinite));
							} catch (RejectedExecutionException ree) {
								logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit dir - " + base + lfnc.child + "/" + ": " + ree);
							}
						}
					}
				}
			}

			// we filter and add the file if -y and metadata
			if (filesVersion != null) {
				HashMap<String, Integer> lfn_version = new HashMap<>();
				HashMap<String, LFN_CSD> lfn_to_csd = new HashMap<>();

				for (LFN_CSD lfnc : filesVersion) {
					Integer version = Integer.valueOf(0);
					String lfn_without_version = lfnc.child;
					if (lfnc.child.lastIndexOf("_v") > 0) {
						lfn_without_version = lfnc.child.substring(0, lfnc.child.lastIndexOf("_v"));
						version = Integer.valueOf(Integer.parseInt(lfnc.child.substring(lfnc.child.indexOf("_v") + 2, lfnc.child.indexOf("_s0"))));
					}
					else
						if (lfnc.metadata.containsKey("CDB__version")) {
							version = Integer.valueOf(lfnc.metadata.get("CDB__version"));
						}

					if (!lfn_version.containsKey(lfn_without_version)) {
						lfn_version.put(lfn_without_version, version);
						lfn_to_csd.put(lfn_without_version, lfnc);
					}

					if (lfn_version.get(lfn_without_version).intValue() < version.intValue()) {
						lfn_version.put(lfn_without_version, version);
						lfn_to_csd.put(lfn_without_version, lfnc);
					}

				}

				for (String lfnc_str : lfn_to_csd.keySet())
					col.add(lfn_to_csd.get(lfnc_str));

			}

			counter_left.decrementAndGet();
		}
	}

	/**
	 * @param base_path
	 * @param pattern
	 * @param flags
	 * @param metadata
	 * @return list of files for find command
	 */
	public static Collection<LFN_CSD> find(final String base_path, final String pattern, final int flags, final String metadata) {
		return recurseAndFilterLFNs("find", base_path, "*" + pattern, metadata, flags);
	}

	/**
	 * @param path
	 * @param flags
	 * @return list of files for ls command
	 */
	public static Collection<LFN_CSD> ls(final String path, final int flags) {
		final Set<LFN_CSD> ret = new TreeSet<>();

		// if need to resolve wildcard and recurse, we call the recurse method
		if (path.contains("*") || path.contains("?")) {
			return recurseAndFilterLFNs("ls", path, null, null, LFNCSDUtils.FIND_INCLUDE_DIRS);
		}

		// otherwise we should be able to create the LFN_CSD from the path
		LFN_CSD lfnc = new LFN_CSD(path, true, append_table, null, null);
		if (lfnc.isDirectory()) {
			List<LFN_CSD> list = lfnc.list(true, append_table, clevel);
			ret.addAll(list);
		}
		else {
			ret.add(lfnc);
		}

		return ret;
	}

	/**
	 * @param id
	 * @return LFN_CSD that corresponds to the id
	 */
	public static LFN_CSD guid2lfn(final UUID id) {

		String path = "";
		String lfn = "";
		UUID p_id = id;
		UUID p_id_lfn = null;
		HashMap<String, Object> p_c_ids = null;
		boolean first = true;

		while (!p_id.equals(LFN_CSD.root_uuid) && path != null) {
			p_c_ids = LFN_CSD.getInfofromChildId(p_id);

			if (p_c_ids != null) {
				path = (String) p_c_ids.get("path");
				p_id = (UUID) p_c_ids.get("path_id");

				if (first) {
					p_id_lfn = p_id;
					lfn = path;
					first = false;
				}
				else {
					lfn = path + "/" + lfn;
				}
			}
			else {
				logger.severe("LFN_CSD: guid2lfn: Can't get information for id: " + p_id + " - last lfn: " + lfn);
				return null;
			}
		}

		lfn = "/" + lfn;
		return new LFN_CSD(lfn, true, null, p_id_lfn, id);
	}

	/**
	 * @param user
	 * @param source
	 * @param destination
	 * @return final lfn
	 */
	public static Set<LFN_CSD> mv(final AliEnPrincipal user, final String source, final String destination) {
		// Let's assume for now that the source and destination come as absolute paths, otherwise:
		// final String src = FileSystemUtils.getAbsolutePath(user.getName(), (currentDir != null ? currentDir : null), source);
		// final String dst = FileSystemUtils.getAbsolutePath(user.getName(), (currentDir != null ? currentDir : null), destination);
		if (source.equals(destination)) {
			logger.info("LFNCSDUtils: mv: the source and destination are the same: " + source + " -> " + destination);
			return null;
		}

		TreeSet<LFN_CSD> lfnc_final = new TreeSet<>();
		String[] destination_parts = LFN_CSD.getPathAndChildFromCanonicalName(destination);
		LFN_CSD lfnc_target_parent = new LFN_CSD(destination_parts[0], true, null, null, null);
		LFN_CSD lfnc_target = new LFN_CSD(destination, true, null, lfnc_target_parent.id, null);

		if (!lfnc_target_parent.exists) {
			logger.info("LFNCSDUtils: mv: the destination doesn't exist: " + destination);
			return null;
		}
		if (!AuthorizationChecker.canWrite(lfnc_target_parent, user)) {
			logger.info("LFNCSDUtils: mv: no permission on the destination: " + destination);
			return null;
		}

		// expand wildcards and filter if needed
		if (source.contains("*") || source.contains("?")) {
			Collection<LFN_CSD> lfnsToMv = recurseAndFilterLFNs("mv", source, null, null, LFNCSDUtils.FIND_INCLUDE_DIRS);

			for (LFN_CSD l : lfnsToMv) {
				// check permissions to move
				if (!AuthorizationChecker.canWrite(l, user)) {
					logger.info("LFNCSDUtils: mv: no permission on the source: " + l.getCanonicalName());
					return null;
				}
				// move and add to the final collection of lfns
				LFN_CSD lfncf = LFN_CSD.mv(l, lfnc_target, lfnc_target_parent);
				if (lfncf != null)
					lfnc_final.add(lfncf);
			}
		}
		else {
			LFN_CSD lfnc_source = new LFN_CSD(source, true, null, null, null);
			// check permissions to move
			if (!AuthorizationChecker.canWrite(lfnc_source, user)) {
				logger.info("LFNCSDUtils: mv: no permission on the source: " + source);
				return null;
			}
			// move and add to the final collection of lfns
			LFN_CSD lfncf = LFN_CSD.mv(lfnc_source, lfnc_target, lfnc_target_parent);
			if (lfncf != null)
				lfnc_final.add(lfncf);
		}

		return lfnc_final;
	}

}
