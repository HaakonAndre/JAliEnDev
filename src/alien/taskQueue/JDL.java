package alien.taskQueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lazyj.Utils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.io.IOUtils;

/**
 * @author costing
 * 
 */
public class JDL implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4803377858842338873L;
	private final Map<String, Object> jdlContent = new HashMap<String, Object>();

	/**
	 * A file in the catalogue
	 * 
	 * @param file
	 * @throws IOException
	 */
	public JDL(final GUID file) throws IOException {
		this(IOUtils.getContents(file));
	}

	/**
	 * a local file
	 * 
	 * @param file
	 * @throws IOException
	 */
	public JDL(final File file) throws IOException {
		this(Utils.readFile(file.getAbsolutePath()));
	}

	/**
	 * the full contents
	 * 
	 * @param content
	 * @throws IOException
	 */
	public JDL(final String content) throws IOException {
		if (content == null || content.length() == 0) {
			throw new IOException("Content is "
					+ (content == null ? "null" : "empty"));
		}

		int iPrevPos = 0;

		int idxEqual = -1;

		while ((idxEqual = content.indexOf('=', iPrevPos + 1)) > 0) {
			final String sKey = clean(content.substring(iPrevPos, idxEqual)
					.trim());

			int idxEnd = idxEqual + 1;

			boolean bEsc = false;
			boolean bQuote = false;

			outer: while (idxEnd < content.length()) {
				final char c = content.charAt(idxEnd);

				switch (c) {
				case '\\':
					bEsc = !bEsc;

					break;
				case '"':
					if (!bEsc)
						bQuote = !bQuote;

					bEsc = false;

					break;
				case ';':
					if (!bEsc && !bQuote)
						break outer;

					bEsc = false;

					break;
				default:
					bEsc = false;
				}

				idxEnd++;
			}

			final String sValue = content.substring(idxEqual + 1, idxEnd)
					.trim();

			final Object value = parseValue(sValue);

			// System.err.println(sKey +" = "+value);

			if (value != null) {
				jdlContent.put(sKey.toLowerCase(), value);
			}

			iPrevPos = idxEnd + 1;
		}
	}

	private String clean(final String input) {
		String output = input;

		while (output.startsWith("#")) {
			int idx = output.indexOf('\n');

			if (idx < 0)
				return "";

			output = output.substring(idx + 1);
		}

		while (output.startsWith("\n"))
			output = output.substring(1);

		while (output.endsWith("\n"))
			output = output.substring(0, output.length() - 1);

		return output;
	}

	/**
	 * Get the value of a key
	 * 
	 * @param key
	 * 
	 * @return the value, can be a String, a List ...
	 */
	public Object get(final String key) {
		return jdlContent.get(key.toLowerCase());
	}

	/**
	 * Get the value of this key as String
	 * 
	 * @param key
	 * 
	 * @return the single value if this was a String, the first entry of a
	 *         List...
	 */
	public String gets(final String key) {
		final Object o = get(key);

		return getString(o);
	}

	private static String getString(final Object o) {
		if (o == null)
			return null;

		if (o instanceof String)
			return (String) o;

		if (o instanceof List<?>) {
			final List<?> l = (List<?>) o;

			if (l.size() > 0)
				return getString(l.get(0));
		}

		return o.toString();
	}

	private static final Object parseValue(final String value) {
		if (value.startsWith("\"") && value.endsWith("\"")) {
			return value.substring(1, value.length() - 1);
		}

		if (value.startsWith("{") && value.endsWith("}")) {
			return toList(value.substring(1, value.length() - 1));
		}

		return value;
	}

	private static final Pattern PANDA_RUN_NO = Pattern
			.compile(".*/run(\\d+)$");

	/**
	 * Get the run number if this job is a simulation job
	 * 
	 * @return run number
	 */
	public int getSimRun() {
		final String split = gets("splitarguments");

		if (split == null) {
			// is it a Panda production ?

			final String sOutputDir = getOutputDir();

			if (sOutputDir == null || sOutputDir.length() == 0)
				return -1;

			final Matcher m = PANDA_RUN_NO.matcher(sOutputDir);

			if (m.matches())
				return Integer.parseInt(m.group(1));

			return -1;
		}

		if (split.indexOf("sim") < 0) {
			return -1;
		}

		final StringTokenizer st = new StringTokenizer(split);

		while (st.hasMoreTokens()) {
			final String s = st.nextToken();

			if (s.equals("--run")) {
				if (st.hasMoreTokens()) {
					final String run = st.nextToken();

					try {
						return Integer.parseInt(run);
					} catch (NumberFormatException nfe) {
						return -1;
					}
				}

				return -1;
			}
		}

		return -1;
	}

	/**
	 * Get the number of jobs this masterjob will split into. Only works for
	 * productions that split in a fixed number of jobs.
	 * 
	 * @return the number of subjobs
	 */
	public int getSplitCount() {
		final String split = gets("split");

		if (split == null || split.length() == 0)
			return -1;

		if (split.startsWith("production:")) {
			try {
				return Integer
						.parseInt(split.substring(split.lastIndexOf('-') + 1));
			} catch (NumberFormatException nfe) {
				// ignore
			}
		}

		return -1;
	}

	/**
	 * Get the list of input files
	 * 
	 * @return the list of input files
	 */
	public List<String> getInputFiles() {
		return getInputFiles(true);
	}

	/**
	 * @return the input data
	 */
	public List<String> getInputData() {
		return getInputData(true);
	}

	/**
	 * Get the list of input data
	 * 
	 * @param bNodownloadIncluded
	 *            include or not the files with the ",nodownload" option
	 * 
	 * @return list of input data to the job
	 */
	public List<String> getInputData(final boolean bNodownloadIncluded) {
		return getInputList(bNodownloadIncluded, "InputData");
	}

	/**
	 * Get the list of input files
	 * 
	 * @param bNodownloadIncluded
	 *            flag to include/exclude the files for which ",nodownload" is
	 *            indicated in the JDL
	 * @return list of input files
	 */
	public List<String> getInputFiles(final boolean bNodownloadIncluded) {
		List<String> ret = getInputList(bNodownloadIncluded, "InputFile");

		if (ret == null)
			ret = getInputList(bNodownloadIncluded, "InputBox");

		return ret;
	}

	/**
	 * Get the list of output files
	 * 
	 * @return list of output files
	 */
	public List<String> getOutputFiles() {
		List<String> ret = getInputList(false, "Output");
		if (ret == null)
			ret = new LinkedList<String>();
		List<String> retf = getInputList(false, "OutputFile");
		if (retf != null)
			ret.addAll(retf);
		List<String> reta = getInputList(false, "OutputArchive");
		if (reta != null)
			ret.addAll(retf);

		return ret;
	}

	
	/**
	 * Get the list of arguments
	 * 
	 * @return list of arguments
	 */
	public List<String> getArguments() {
		return getInputList(false, "Arguments");
	}
	
	/**
	 * Get the executable
	 * 
	 * @return executable
	 */
	public List<String> getExecutable() {
		return getInputList(false, "Executeable");
	}

	/**
	 * Get the output directory
	 * 
	 * @return output directory
	 */
	public List<String> getOutputDirectory() {
		return getInputList(false, "OutputDir");
	}
	
	/**
	 * Get the list of input files for a given tag
	 * 
	 * @param bNodownloadIncluded
	 *            flag to include/exclude the files for which ",nodownload" is
	 *            indicated in the JDL
	 * @param sTag
	 *            tag to extract the list from
	 * @return input list
	 */
	public List<String> getInputList(final boolean bNodownloadIncluded,
			final String sTag) {
		final Object o = get(sTag);

		if (o == null) {
			// System.err.println("No such tag: "+sTag);
			// for (Map.Entry<?, ?> me: jdlContent.entrySet()){
			// System.err.println("'"+me.getKey()+"' = '"+me.getValue()+"'");
			// }

			return null;
		}

		final List<String> ret = new LinkedList<String>();

		if (o instanceof String) {
			final String s = (String) o;

			if (bNodownloadIncluded || s.indexOf(",nodownload") < 0)
				ret.add(removeLF(s));

			return ret;
		}

		if (o instanceof List<?>) {
			final Iterator<?> it = ((List<?>) o).iterator();

			while (it.hasNext()) {
				final Object o2 = it.next();

				if (o2 instanceof String) {
					final String s = (String) o2;

					if (bNodownloadIncluded || s.indexOf(",nodownload") < 0)
						ret.add(removeLF(s));
				}
			}
		}

		return ret;
	}

	private static String removeLF(final String s) {
		String ret = s;

		if (ret.startsWith("LF:"))
			ret = ret.substring(3);

		int idx = ret.indexOf(",nodownload");

		if (idx >= 0)
			ret = ret.substring(0, idx);

		return ret;
	}

	private static List<String> toList(final String value) {
		final List<String> ret = new LinkedList<String>();

		int idx = value.indexOf('"');

		if (idx < 0) {
			ret.add(value);
			return ret;
		}

		do {
			int idx2 = value.indexOf('"', idx + 1);

			if (idx2 < 0)
				return ret;

			while (value.charAt(idx2 - 1) == '\'') {
				idx2 = value.indexOf('"', idx2 + 1);

				if (idx2 < 0)
					return ret;
			}

			ret.add(value.substring(idx + 1, idx2));

			idx = value.indexOf('"', idx2 + 1);
		} while (idx > 0);

		return ret;
	}

	/**
	 * Get the job comment
	 * 
	 * @return job comment
	 */
	public String getComment() {
		final String sType = gets("Jobtag");

		if (sType == null)
			return null;

		if (sType.toLowerCase().startsWith("comment:"))
			return sType.substring(8).trim();

		return sType.trim();
	}

	/**
	 * Get the (package, version) mapping. Ex: { (AliRoot -> v4-19-16-AN), (ROOT
	 * -> v5-26-00b-6) }
	 * 
	 * @return packages
	 */
	@SuppressWarnings("unchecked")
	public Map<String, String> getPackages() {
		final Object o = get("Packages");

		if (!(o instanceof List))
			return null;

		final Iterator<String> it = ((List<String>) o).iterator();

		final Map<String, String> ret = new HashMap<String, String>();

		while (it.hasNext()) {
			final String s = it.next();

			try {
				int idx = s.indexOf('@');

				final int idx2 = s.indexOf("::", idx + 1);

				final String sPackage = s.substring(idx + 1, idx2);

				final String sVersion = s.substring(idx2 + 2);

				ret.put(sPackage, sVersion);
			} catch (Throwable t) {
				System.err
						.println("Exception parsing package definition: " + s);
			}
		}

		return ret;
	}

	/**
	 * Get the output directory
	 * 
	 * @return output directory
	 */
	public String getOutputDir() {
		String s = gets("OutputDir");

		if (s == null)
			return null;

		int idx = s.indexOf("#alien");

		if (idx >= 0) {
			int idxEnd = s.indexOf("#", idx + 1);

			if (idxEnd > 0)
				s = s.substring(0, idx);
		}

		if (s.endsWith("/"))
			s = s.substring(0, s.length() - 1);

		return s;
	}

	/**
	 * Get the number of events/job in this simulation run
	 * 
	 * @return events/job, of -1 if not supported
	 */
	public int getSimFactor() {
		final List<String> inputFiles = getInputFiles();

		if (inputFiles == null)
			return -1;

		for (String file : inputFiles) {
			if (file.endsWith("sim.C"))
				return getSimFactor(LFNUtils.getLFN(file));
		}

		return -1;
	}

	// void sim(Int_t nev=300) {
	// void sim(Int_t nev = 300) {
	private static final Pattern pSimEvents = Pattern
			.compile(".*void.*sim.*\\s+n(\\_)?ev\\s*=\\s*(\\d+).*");

	/**
	 * Get the number of events/job that this macro is expected to produce
	 * 
	 * @param f
	 * @return events/job, or -1 if not supported
	 */
	public static int getSimFactor(final LFN f) {
		GUID guid = GUIDUtils.getGUID(f.guid);

		if (guid == null)
			return -1;

		final String sContent = IOUtils.getContents(guid);

		try {
			final BufferedReader br = new BufferedReader(new StringReader(
					sContent));

			String sLine;

			while ((sLine = br.readLine()) != null) {
				Matcher m = pSimEvents.matcher(sLine);

				if (m.matches())
					return Integer.parseInt(m.group(2));
			}
		} catch (IOException ioe) {
			// ignore, cannot happen
		}

		return -1;
	}
}
