package alien.site;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import alien.taskQueue.JDL;
import lia.util.Utils;

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
				jobOutput.add(new OutputEntry(archparts[0], filesincluded, options, Long.valueOf(queueId)));
			}
			else {
				// file(s)
				System.out.println("Single file: " + parts[0]);
				final ArrayList<String> filesincluded = parsePatternFiles(parts[0].split(","));
				for (final String f : filesincluded) {
					System.out.println("Adding single: [" + f + "] and opt: [" + options + "]");
					jobOutput.add(new OutputEntry(f, null, options, Long.valueOf(queueId)));
				}
			}
		}

		System.out.println(jobOutput.toString());

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
						for (String f : parts) {
							f = f.trim();
							if (f.length() > 0) {
								final String fname = new File(f).getName();
								System.out.println("Adding file from ls: " + fname);
								filesFound.add(fname);
							}
						}
				}
				else
					filesFound.add(file);
			}

		System.out.println("Returned parsed array: " + filesFound.toString());

		return filesFound;
	}

	/**
	 * @return list of entries
	 */
	public ArrayList<OutputEntry> getEntries() {
		return this.jobOutput;
	}

}
