package alien.taskQueue.jobsplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;

import alien.catalogue.LFN;
import alien.taskQueue.JDL;

public class SplitParse extends JobSplitter {
	
	private final String strategy;
	
	public SplitParse (String strategy) {
		this.strategy = strategy;
	}

	public List<JDL> splitJobs(final JDL j, long masterId) throws IOException {
		
		logger.log(Level.FINE, "Splitting by parsing!");

		Map<String, List<String>> inputFiles = groupInputFiles(j);

		int maxinputsize = 0;
		int maxinputnumber = 0;

		if (j.gets("SplitMaxInputFileSize") != null) {
			maxinputsize = Integer.parseInt(j.gets("SplitMaxInputFileSize"));
		}
		else if (j.gets("SplitMaxInputFileNumber") != null) {
			maxinputnumber = Integer.parseInt(j.gets("SplitMaxInputFileNumber"));
		}
		int currentsize = 0;
		int currentnumber = 0;

		List<JDL> subjobs = new ArrayList<JDL>();

		for (Entry<String, List<String>> entry : inputFiles.entrySet()) {
			List<String> tempInput = new ArrayList<String>();
			JDL tempjob = new JDL();
			for (String file : entry.getValue()) {
				currentnumber++;
				if (maxinputnumber < currentnumber && maxinputnumber != 0) {
					if (!tempInput.isEmpty()) {
						tempjob = prepareSubJobJDL(j, masterId, 0, tempInput);
						subjobs.add(tempjob);
						currentnumber = 0;
						currentsize = 0;
					}

				} /*
					 * else if (maxinputsize < currentsize) { subjob.setChanges("InputFiles",
					 * tempInput, "Change"); subjobs.add(subjob); currentnumber = 0; currentsize =
					 * 0; }
					 */
				
				tempInput.add(file);
			}
			
			tempjob = prepareSubJobJDL(j, masterId, 0, tempInput);
			subjobs.add(tempjob);
		}

		return subjobs;
	}

	public Map<String, List<String>> groupInputFiles(JDL j) throws IOException {
		Map<String, List<String>> groupedFiles = new HashMap<String, List<String>>();
		Collection<LFN> inputFiles = getInputFiles(j);

		for (LFN file : inputFiles) {

			String fileS = file.getCanonicalName();
			String newFile = JobSplitHelper.replaceAllString(strategy, fileS, "");

			if (!groupedFiles.containsKey(newFile)) {
				groupedFiles.put(newFile, new ArrayList<String>());
			}
			logger.log(Level.FINE, "InputFile added is: " + newFile);
			groupedFiles.get(newFile).add(fileS);
		}

		return groupedFiles;
	}
}
