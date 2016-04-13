package alien.site;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import alien.taskQueue.JDL;
import lia.util.Utils;

class OutputEntry {
	private final String name;
	private final ArrayList<String> filesIncluded;
	private final String options;
	private boolean isRootArchive;
	private final ArrayList<String> ses;
	private final ArrayList<String> exses;
	private final HashMap<String, Integer> qos;

	public OutputEntry(final String name, final ArrayList<String> filesIncluded, final String options) {
		this.name = name;
		this.filesIncluded = filesIncluded;
		this.options = options;
		this.isRootArchive = false;
		this.ses = new ArrayList<>();
		this.exses = new ArrayList<>();
		this.qos = new HashMap<>();

		if (this.filesIncluded != null)
			for (final String f : this.filesIncluded)
				if (f.endsWith(".root")) {
					this.isRootArchive = true;
					break;
				}

		// parse options
		if (this.options.length() > 0) {
			final String[] opts = this.options.split(",");

			for (final String o : opts) {
				System.out.println("Parsing option: " + o);

				if (o.contains("=")) {
					// e.g. disk=2
					final String[] qosparts = o.split("=");
					qos.put(qosparts[0], Integer.valueOf(qosparts[1]));
				} else if (o.contains("!"))
					// de-prioritized se
					exses.add(o.substring(1));
				else
					// prioritized se
					ses.add(o);
			}
		}

		System.out.println("QoS: " + qos.toString());
		System.out.println("SEs: " + ses.toString());
		System.out.println("ExSEs: " + exses.toString());

	}

	public String getName() {
		return this.name;
	}

	public ArrayList<String> getSEsPrioritized() {
		return ses;
	}

	public ArrayList<String> getSEsDeprioritized() {
		return exses;
	}

	public HashMap<String, Integer> getQoS() {
		return qos;
	}

	public ArrayList<String> getFilesIncluded() {
		return this.filesIncluded;
	}

	public String getOptions() {
		return this.options;
	}

	public boolean isArchive() {
		return this.filesIncluded != null && this.filesIncluded.size() > 0;
	}

	public void createZip(String path) {
		if (path == null)
			path = System.getProperty("user.dir");
		if (!path.endsWith("/"))
			path += "/";

		if (this.filesIncluded == null)
			return;

		try {
			// output file
			final ZipOutputStream out = new ZipOutputStream(new FileOutputStream(path + this.name));
			if (this.isRootArchive)
				out.setLevel(0);

			boolean hasPhysicalFiles = false;

			for (final String file : this.filesIncluded) {
				final File f = new File(path + file);
				if (!f.exists() || !f.isFile() || !f.canRead() || f.length() <= 0) {
					System.out.println("File " + file + " doesn't exist, cannot be read or has 0 size!");
					continue;
				}
				hasPhysicalFiles = true;

				// input file
				final FileInputStream in = new FileInputStream(path + file);
				// name of the file inside the zip file
				out.putNextEntry(new ZipEntry(file));

				final byte[] b = new byte[1024];
				int count;

				while ((count = in.read(b)) > 0)
					out.write(b, 0, count);
				in.close();
			}
			out.close();

			if (!hasPhysicalFiles)
				Files.delete(Paths.get(path + this.name));

		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		} catch (final IOException e) {
			e.printStackTrace();
		}

		return;
	}

	@Override
	public String toString() {
		String toString = "Name: " + this.name + " Options: " + this.options + " Files?: ";
		if (this.filesIncluded != null)
			toString += this.filesIncluded.toString();
		return toString;
	}
}

/**
 * @author costing
 *
 */
public class ParsedOutput {
	private final ArrayList<OutputEntry> jobOutput;
	private final JDL jdl;
	private final long queueId;
	private final String pwd;
	private final String tag;

	/**
	 * @param queueId
	 * @param jdl
	 */
	public ParsedOutput(final long queueId, final JDL jdl) {
		this.jobOutput = new ArrayList<>();
		this.jdl = jdl;
		this.queueId = queueId;
		this.pwd = "";
		this.tag = "Output";
		parseOutput();
	}

	/**
	 * @param queueId
	 * @param jdl
	 * @param path
	 */
	public ParsedOutput(final long queueId, final JDL jdl, final String path) {
		this.jobOutput = new ArrayList<>();
		this.jdl = jdl;
		this.queueId = queueId;
		this.pwd = path + "/";
		this.tag = "Output";
		parseOutput();
	}

	/**
	 * @param queueId
	 * @param jdl
	 * @param path
	 * @param tag
	 */
	public ParsedOutput(final long queueId, final JDL jdl, final String path, final String tag) {
		this.jobOutput = new ArrayList<>();
		this.jdl = jdl;
		this.queueId = queueId;
		this.pwd = path + "/";
		this.tag = tag;
		parseOutput();
	}

	// TODELETE SYSOUTS

	/**
	 *
	 */
	public void parseOutput() {
		final List<String> files = jdl.getOutputFiles(this.tag);

		if (files.size() == 0)
			// Create default archive
			files.add("jalien_defarchNOSPEC." + this.queueId + ":stdout,stderr,resources");
		System.out.println(files); // TODELETE

		for (final String line : files) {

			System.out.println("Line: " + line);

			final String[] parts = line.split("@");

			// System.out.println("Parts: "+parts[0]+" "+parts[1]);

			final String options = parts.length > 1 ? parts[1] : "";

			if (parts[0].contains(":")) {
				// archive
				final String[] archparts = parts[0].split(":");

				System.out.println("Archparts: " + archparts[0] + " " + archparts[1]);

				final ArrayList<String> filesincluded = parsePatternFiles(archparts[1].split(","));

				System.out.println("Adding archive: " + archparts[0] + " and opt: " + options);
				jobOutput.add(new OutputEntry(archparts[0], filesincluded, options));
			} else {
				// file(s)
				System.out.println("Single file: " + parts[0]);
				final ArrayList<String> filesincluded = parsePatternFiles(parts[0].split(","));
				for (final String f : filesincluded) {
					System.out.println("Adding single: [" + f + "] and opt: [" + options + "]");
					jobOutput.add(new OutputEntry(f, null, options));
				}
			}
		}

		System.out.println(jobOutput.toString()); // TODELETE

		return;
	}

	private ArrayList<String> parsePatternFiles(final String[] files) {
		System.out.println("Files to parse patterns: " + Arrays.asList(files).toString());

		final ArrayList<String> filesFound = new ArrayList<>();

		if (!pwd.equals(""))
			for (final String file : files) {
				System.out.println("Going to parse: " + file);
				if (file.contains("*")) {
					final String[] parts = Utils.getOutput("ls " + pwd + file).split("\n");
					if (parts.length > 0)
						for (final String f : parts) {
							f.trim();
							if (f.length() > 0) {
								final String fname = new File(f).getName();
								System.out.println("Adding file from ls: " + fname);
								filesFound.add(fname);
							}
						}
				} else
					filesFound.add(file);
			}

		System.out.println("Returned parsed array: " + filesFound.toString());

		return filesFound;
	}

	/**
	 * @return
	 */
	public ArrayList<OutputEntry> getEntries() {
		return this.jobOutput;
	}

}
