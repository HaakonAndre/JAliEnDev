package alien.site;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import alien.io.IOUtils;

public class OutputEntry {
	private final String name;
	private final ArrayList<String> filesIncluded;
	private HashMap<String, String> md5members = new HashMap<>();
	private HashMap<String, Long> sizemembers = new HashMap<>();
	private final String options;
	private final Long queueId;
	private boolean isRootArchive;
	private final ArrayList<String> ses;
	private final ArrayList<String> exses;
	private final HashMap<String, Integer> qos;

	public OutputEntry(final String name, final ArrayList<String> filesIncluded, final String options, final Long jobid) {
		this.name = name;
		this.filesIncluded = filesIncluded;
		this.options = options;
		this.queueId = jobid;
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

	public Long getQueueId() {
		return this.queueId;
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

	public HashMap<String, Long> getSizesIncluded() {
		return this.sizemembers;
	}
	
	public HashMap<String, String> getMD5sIncluded() {
		return this.md5members;
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

	public ArrayList<String> createZip(String path) {
		if (path == null)
			path = System.getProperty("user.dir");
		if (!path.endsWith("/"))
			path += "/";

		if (this.filesIncluded == null)
			return null;

		try {
			// output file
			final ZipOutputStream out = new ZipOutputStream(new FileOutputStream(path + this.name));
			if (this.isRootArchive)
				out.setLevel(0);

			boolean hasPhysicalFiles = false;

			for (final String file : this.filesIncluded) {
				final File f = new File(path + file);
				if (!f.exists() || !f.isFile() || !f.canRead() || f.length() <= 0) {
					// filesIncluded.remove(file);
					System.out.println("File " + file + " doesn't exist, cannot be read or has 0 size!");
					continue;
				}
				hasPhysicalFiles = true;

				String md5 = null;
				try {
					md5 = IOUtils.getMD5(f);
				} catch (final Exception e1) {
					System.err.println("Error calculating md5 of: " + file);
					// filesIncluded.remove(file);
					continue;
				}

				// Save md5 and size
				sizemembers.put(file, Long.valueOf(f.length()));
				md5members.put(file, md5);

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

		return filesIncluded;
	}

	@Override
	public String toString() {
		String toString = "Name: " + this.name + " Options: " + this.options + " Files?: ";
		if (this.filesIncluded != null)
			toString += this.filesIncluded.toString();
		return toString;
	}
}