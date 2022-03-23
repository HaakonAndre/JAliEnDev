package alien.taskQueue.jobsplit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.sql.SQLException;

import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.XmlCollection;
import alien.api.catalogue.CatalogueApiUtils;
import alien.config.ConfigUtils;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.taskQueue.JDL;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;;

public abstract class JobSplitter {
	
	private JAliEnCOMMander commander; 
	protected CatalogueApiUtils c_api;

	static final Logger logger = ConfigUtils.getLogger(TaskQueueUtils.class.getCanonicalName());

	private int count = 1;
	
	public JobSplitter() {
		commander = JAliEnCOMMander.getInstance();
		c_api = new CatalogueApiUtils(commander);
	}
	

	public abstract List<JDL> splitJobs (final JDL j, long masterId) throws IOException;
	
	@SuppressWarnings("unused")
	public Collection<LFN> getInputFiles(JDL j) throws IOException {
		final List<String> dataFiles = j.getInputData();

		final List<LFN> ret = new LinkedList<>();

		final List<String> otherInputFiles = new LinkedList<>();

		if (dataFiles != null) {
			for (final String file : dataFiles) {
				if (file.endsWith(".xml"))
					try {
						final XmlCollection x = new XmlCollection(LFNUtils.getLFN(file));

						return x;
					} catch (@SuppressWarnings("unused") final IOException ioe) {
						// ignore
					}

				otherInputFiles.add(file);
			}
		}

		final List<LFN> tempList = LFNUtils.getLFNs(true, otherInputFiles);

		if (tempList != null && tempList.size() > 0)
			ret.addAll(tempList);

		if(ret.size() == 0)
			throw new IOException("Split error: no inputfiles to split on");
		return ret;
	}

	JDL prepareSubJobJDL(JDL j, final long masterId, int filesize, List<String> tempInput) {
		
		logger.log(Level.FINE, "Preparing subjob JDL");

		JDL tmpJdl = new JDL();
		
		if(tempInput != null)
			tmpJdl.setChanges("InputData", tempInput, "Change");
		
		String newArg = "";
		if (j.getSplitArguments() != null) {
			List<String> splitArgs = j.getSplitArguments();
			for (String splitArg : splitArgs) {
				newArg = newArg + " " + splitArg;
			}
			String arg = "";
			if (j.gets("Arguments") != null) {
				arg = j.gets("Arguments");
			}
			tmpJdl.setChanges("Arguments",arg + " " + newArg, "Change");
		}
		tmpJdl.setChanges("SplitArguments", "", "Delete");
		tmpJdl.setChanges("Split", "", "Delete");
		tmpJdl.setChanges("MasterJobId", Long.toString(masterId), "Change");
		
		String req = "";
		if(j.gets("Requirements") != null) {
			req = j.gets("Requirements");
		}
		tmpJdl.setChanges("Requirements",
				req + sizeRequirements(filesize, j.gets("WorkDirectorySize")), "Change");

		String outputDir = j.gets("OutputDir");
		List<String> outputFile = j.getInputList(false, "OutputFile"); // Collection
		List<String> outputA = j.getInputList(false, "OutputArchive"); // Collection

		if (outputDir != null) {
			String tmpOutputDir = checkEntryPattern(outputDir, j, tempInput);

			if (tmpOutputDir != "") {
				tmpJdl.setChanges("OutputDir", tmpOutputDir, "Change");
			}
			logger.log(Level.FINE, "Maybe it....");
		}

		if (outputFile != null) {
			logger.log(Level.FINE, "Does it reach?");
			List<String> tmpOutputFiles = new ArrayList<String>();
			boolean outFileChanged = false;
			for (String outF : outputFile) {
				String tmpOutputFile = checkEntryPattern(outF, j, tempInput);
				if (tmpOutputFile != "") {
					tmpOutputFiles.add(tmpOutputFile);
				} else {
					tmpOutputFiles.add(outF);
					outFileChanged = true;
				}
			}
			if (outFileChanged) {
				tmpJdl.setChanges("OutputFile", tmpOutputFiles, "Change");
			}
		}

		if (outputA != null) {
			List<String> tmpOutputArchives = new ArrayList<String>();
			boolean outArchiveChanged = false;
			for (String outA : outputA) {
				String tmpOutputArchive = checkEntryPattern(outA, j, tempInput);
				if (tmpOutputArchive != "") {
					tmpOutputArchives.add(tmpOutputArchive);
				} else {
					tmpOutputArchives.add(outA);
					outArchiveChanged = true;
				}
			}
			if (outArchiveChanged) {
				tmpJdl.setChanges("OutputArchive", tmpOutputArchives, "Change");
			}
		}

		logger.log(Level.FINE, "Done preparing subjob JDL");
		return tmpJdl;
	}

	public String checkEntryPattern(String val, JDL j, List<String> tempInput) {
		
		logger.log(Level.FINE, "Checking Pattern");
		String newEntry = "";
		String origPattern = "\\#alien(\\S+)\\#";
		Pattern p = Pattern.compile(origPattern, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(val);
		if (m.find()) {
			String tmpPattern = m.group(1);
			String newpattern = "";
			if(tempInput != null) {
			String filePattern = checkFilePattern(tmpPattern, j, tempInput);
			if (filePattern != "") {
				newpattern = filePattern;
			}
			}

			String pattern = "^_counter_(.*)$";
			p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
			m = p.matcher(tmpPattern);
			if (m.find()) {
				if (m.group(1) != "") {
					newpattern = "%" + JobSplitHelper.replaceAllString("i", m.group(1), "d");
				} else {
					newpattern = "%d";
				}
				newpattern = String.format(newpattern, count);
				count++;
			}
			newEntry = JobSplitHelper.replaceAllString("\\#alien(\\S+)\\#", val, newpattern);

		}

		return newEntry;
	}

	public String checkFilePattern(String val, JDL j, List<String>tempInput) {
		logger.log(Level.FINE, "Checking File pattern");
		String file = "";
		boolean all = false;
		if (JobSplitHelper.patternMatches("^first", val)) {
			file = tempInput.get(0);
			file = JobSplitHelper.replaceAllString("^LF:", file, "");
			file = JobSplitHelper.replaceAllString(",nodownload", file, "");
		} else if (JobSplitHelper.patternMatches("^last", val)) {
			file = tempInput.get(tempInput.size() - 1);
			file = JobSplitHelper.replaceAllString("^LF:", file, "");
			file = JobSplitHelper.replaceAllString(",nodownload", file, "");
		} else if (JobSplitHelper.patternMatches("^all", val)) {
			file = String.join(",", tempInput);
			file = JobSplitHelper.replaceAllString("^LF:", file, "");
			file = JobSplitHelper.replaceAllString(",nodownload", file, "");
			all = true;
		} else if (!tempInput.isEmpty()) {
			file = tempInput.get(0);
			file = JobSplitHelper.replaceAllString("^LF:", file, "");
			file = JobSplitHelper.replaceAllString(",nodownload", file, "");
		}

		if (JobSplitHelper.patternMatches("^fulldir", val)) {
			return file;
		} else if (JobSplitHelper.patternMatches("^(dir)+$", val)) {
			while (JobSplitHelper.patternMatches("^dir", val)) {
				val = JobSplitHelper.replaceFirstString("^dir", val, "");
				file = JobSplitHelper.replaceAllString("\\/[^\\/]*$", file, "");
			}
			file = JobSplitHelper.replaceAllString("^.*\\/", file, "");
			return file;
		} else if (JobSplitHelper.patternMatches("^filename(/(.*)/(.*)/)?$", val)) {
			List<String> allFile = new ArrayList<String>();
			Pattern p = Pattern.compile("^filename(/(.*)/(.*)/)?$", Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(val);
			if (all) {
				allFile = tempInput;
			} else {
				allFile.add(file);
			}

			for (String f : allFile) {
				f = JobSplitHelper.replaceAllString("^.*\\/", file, "");
				if (m.group(1) != "" && m.group(2) != "") {
					f = JobSplitHelper.replaceAllString(m.group(1), f, m.group(2));
				}
			}
			return String.join(",", allFile);
		}

		return file;

	}

	public String sizeRequirements(int size, String workSpace) {
		
		logger.log(Level.FINE, "Checking size requirements");

		if (size != 0) {
			size = ((size / (1024 * 8192)) + 1) * 8192;
		}

		if (workSpace != null) {
			int space = 0;
			if (JobSplitHelper.patternMatches("MB", workSpace)) {
				space = Integer.parseInt(JobSplitHelper.replaceAllString("MB", workSpace, ""));
				space = space * 1024;
			}

			else if (JobSplitHelper.patternMatches("GB", workSpace)) {
				space = Integer.parseInt(JobSplitHelper.replaceAllString("GB", workSpace, ""));
				space = space * 1024 * 1024;
			}

			if (space > size) {
				size = space;
			}
		}

		return ("&& ( ( other.LocalDiskSpace > " + size + " ) )");
	}


}
