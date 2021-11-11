package alien.taskQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.sql.SQLException;

import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.LFN;
import alien.config.ConfigUtils;

import alien.shell.commands.JAliEnCOMMander;
import alien.user.AliEnPrincipal;;

public class JobOptimizer {

	public List<JDL> OptimizeJob(final JDL j, final AliEnPrincipal account, final String owner, final long queueId) {

		List<JDL> subjobs = new ArrayList<JDL>();
		if (j.gets("split").startsWith("production:")) {
			// if (!getQuota(j.getSubJobs()) return -1
			subjobs = productionSplit(j, queueId);
		} else if (j.gets("split").startsWith("userdefined")) {
			return null;
		} else {
			String strategy = getSplitStrategy(j.gets("split"));
			if (strategy != "") {
				subjobs = SplitJobs(j, strategy);
			}
		}
		try {
			TaskQueueUtils.insertSubJob(account, queueId, subjobs);
		} catch (final IOException | SQLException ioe) {
			throw new IllegalArgumentException(ioe.getMessage());
		}

		return subjobs;
	}

	String getSplitStrategy(String strategy) {
		switch (strategy) {
		case "parentdirectory":
			return "\\/[^\\/]*\\/?[^\\/]*$";
		case "directory":
			return "\\/[^\\/]*$";
		case "file":
			return "\\/[^\\/]*$";
		default:
			return "";

		}
	}

	public List<JDL> SplitJobs(final JDL j, String strategy) {

		List<String> inputFiles = new ArrayList<String>(); // getInputData(j.getInputData());
		Map<String, List<String>> fileSort = new HashMap<String, List<String>>();
		for (String file : inputFiles) {
			String newFile = replaceAllString(strategy, file, "");
			if (!fileSort.containsKey(newFile)) {
				fileSort.put(newFile, new ArrayList<String>(Arrays.asList(file)));
			} else {
				fileSort.get(newFile).add(file);
			}
		}
		List<JDL> subjobs = new ArrayList<JDL>();

		String maxinputsize = j.gets("SplitMaxInputFileSize");
		String maxinputnumber = j.gets("SplitMaxInputFileNumber");
		int currentsize = 0;
		int currentnumber = 0;

		JDL temp = new JDL();

		for (List<String> files : fileSort.values()) {
			for (String file : files) {
				boolean newSubjob = false;
				if (maxinputnumber != "") {
					if (Integer.parseInt(maxinputnumber) < currentnumber) {
						subjobs.add(temp);
						temp = new JDL();
						newSubjob = true;
					}
				} else if (maxinputsize != "" && !newSubjob) {
					if (Integer.parseInt(maxinputsize) < currentsize) {
						subjobs.add(temp);
						temp = new JDL();
						newSubjob = true;
					}
				}
				temp.append("InputData", file);
			}
			subjobs.add(temp);
			temp = new JDL();
		}
		long queueid = 13336;
		subjobs = prepareSubJobJDL(j, subjobs, queueid, 0);

		return subjobs;
	}

	public Collection<LFN> getInputData(String inputData) {
		List<String> files = new ArrayList<String>();
		if (inputData.matches("\\*")) {
			Pattern r = Pattern.compile("^([^\\*]*)\\*(.*)$");
			if (inputData.matches("^([^\\*]*)\\*(.*)$")) {
				Matcher m = r.matcher(inputData);
				String dir = m.group(0);
				r = Pattern.compile("/LF:/");
				r.matcher(dir).replaceAll("");
				String name = m.group(1);

				Collection<LFN> inputCollection;
				
				if (name.matches("(.*)\\[(\\d*)\\-(\\d*)\\]")) {
					r = Pattern.compile("(.*)\\[(\\d*)\\-(\\d*)\\]");
					name = m.group(0);
					int start = Integer.parseInt(m.group(1));
					int stop = Integer.parseInt(m.group(2));
					inputCollection = commander.c_api.find(dir, name, 0);
					
					
				} else {
					Collection<LFN> inputCollection = commander.c_api.find(dir, name, 0);

				}

			}

		}

		return inputCollection;
	}

	public List<JDL> productionSplit(JDL j, final long queueId) { // Use already existing, rewrite
		String pattern = "^production:(.+)-(.+)";
		Pattern r = Pattern.compile(pattern);

		System.out.println(j.gets("split"));

		Matcher m = r.matcher(j.gets("split"));

		int start = 0;
		int end = 0;

		if (m.find()) {
			start = Integer.parseInt(m.group(1));
			end = Integer.parseInt(m.group(2));
		}

		else {
			System.out.println("No Capturing group");
			return null;
		}

		List<JDL> jdls = Collections.nCopies(end - start, new JDL());

		List<JDL> jdlsFinal = prepareSubJobJDL(j, jdls, queueId, 0);

		return jdlsFinal;
	}

	List<JDL> prepareSubJobJDL(JDL orig, List<JDL> subjobs, final long queueId, int filesize) {

		int i = 0;
		for (JDL j : subjobs) {

			String outPutDir = orig.gets("OutputDir");
			String outPutFile = orig.gets("OutputFile");
			String outPutA = orig.gets("OutputArchive");
			List<String> newArg = new ArrayList<String>();
			if (j.getArguments() != null) {
				newArg.addAll(j.getArguments());
			}
			if (j.getSplitArguments() != null) {
				newArg.addAll(j.getSplitArguments());
			}
			j.setDeleteChanges("SplitArguments");
			j.setDeleteChanges("Split");
			j.setChanges("MasterJobId", Long.toString(queueId));
			String newArgs = "";
			for (String arg : newArg) {
				newArgs = String.format("%s%s ", newArgs, checkEntryPattern(arg, i, j));
			}
			j.setChanges("Requirements", newArgs + sizeRequirements(filesize, j.gets("WorkDirectorySize")));
			j.setChanges("OutputDir", checkEntryPattern(outPutDir, i, j));
			j.setChanges("OutputFile", checkEntryPattern(outPutFile, i, j));
			j.setChanges("OutputArchive", checkEntryPattern(outPutA, i, j));

		}
		return subjobs;
	}

	public String checkEntryPattern(String val, int counter, JDL j) {
		String newEntry = "";
		String origPattern = "\\#alien(\\S+)\\#";
		Pattern p = Pattern.compile(origPattern, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(val);
		if (m.find()) {
			String tmpPattern = m.group(1);
			String newpattern = "";
			String filePattern = checkFilePattern(tmpPattern, j);
			if (filePattern != "") {
				newpattern = filePattern;
			}
			System.out.println("tmpPattern :" + tmpPattern);

			String pattern = "^_(counter)_?(.*)$";
			p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
			m = p.matcher(tmpPattern);
			if (m.find()) {
				if (m.group(2) != "") {
					String format = "%" + replaceAllString("i", m.group(2), "d");
					System.out.println("Format: " + format);
					newpattern = String.format(format, counter);
				} else {
					newpattern = String.format("%d", counter);
					System.out.println(newpattern);
				}
			}
			newEntry = replaceAllString("\\#alien(\\S+)\\#", val, newpattern);
			System.out.println("New Entry: " + newEntry);

		}

		return newEntry;
	}

	public String checkFilePattern(String val, JDL j) {
		String file = "";
		boolean all = false;
		if (patternMatches("^first", val)) {
			file = j.getInputFiles().get(0);
			file = replaceAllString("^LF:", file, "");
			file = replaceAllString(",nodownload", file, "");
		} else if (patternMatches("^last", val)) {
			file = j.getInputFiles().get(j.getInputFiles().size() - 1);
			file = replaceAllString("^LF:", file, "");
			file = replaceAllString(",nodownload", file, "");
		} else if (patternMatches("^all", val)) {
			file = String.join(",", j.getInputFiles());
			file = replaceAllString("^LF:", file, "");
			file = replaceAllString(",nodownload", file, "");
			all = true;
		} else if (!j.getInputFiles().isEmpty()) {
			file = j.getInputFiles().get(0);
			file = replaceAllString("^LF:", file, "");
			file = replaceAllString(",nodownload", file, "");
		}

		if (patternMatches("^fulldir", val)) {
			return file;
		} else if (patternMatches("^(dir)+$", val)) {
			while (patternMatches("^dir", val)) {
				val = replaceFirstString("^dir", val, "");
				file = replaceAllString("\\/[^\\/]*$", file, "");
			}
			file = replaceAllString("^.*\\/", file, "");
			return file;
		} else if (patternMatches("^filename(/(.*)/(.*)/)?$", val)) {
			List<String> allFile = new ArrayList<String>();
			Pattern p = Pattern.compile("^filename(/(.*)/(.*)/)?$", Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(val);
			if (all) {
				allFile = j.getInputFiles();
			} else {
				allFile.add(file);
			}

			for (String f : allFile) {
				f = replaceAllString("^.*\\/", file, "");
				if (m.group(1) != "" && m.group(2) != "") {
					f = replaceAllString(m.group(1), f, m.group(2));
				}
			}
			return String.join(",", allFile);
		}

		return file;

	}

	public String sizeRequirements(int size, String workSpace) {

		if (size != 0) {
			size = ((size / (1024 * 8192)) + 1) * 8192;
		}

		if (workSpace != null) {
			System.out.println("WorkDirectorySize = " + workSpace);
			int space = 0;
			if (patternMatches("MB", workSpace)) {
				space = Integer.parseInt(replaceAllString("MB", workSpace, ""));
				System.out.println("In MB: " + space);
				space = space * 1024;
			}

			else if (patternMatches("GB", workSpace)) {
				space = Integer.parseInt(replaceAllString("GB", workSpace, ""));
				System.out.println("In GB: " + space);
				space = space * 1024 * 1024;
			}

			if (space > size) {
				size = space;
			}
		}

		return ("&& ( ( other.LocalDiskSpace > " + size + " ) )");
	}

	public String replaceAllString(String pattern, String s, String replace) {
		Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(s);
		return m.replaceAll(replace);
	}

	public String replaceFirstString(String pattern, String s, String replace) {
		Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(s);
		return m.replaceFirst(replace);
	}

	public boolean patternMatches(String pattern, String s) {
		Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(s);
		if (m.find()) {
			return true;
		}
		return false;
	}

}
