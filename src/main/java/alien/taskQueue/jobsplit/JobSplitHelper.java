package alien.taskQueue.jobsplit;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.config.ConfigUtils;
import alien.taskQueue.JDL;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JobSplitHelper {

	static final Logger logger = ConfigUtils.getLogger(TaskQueueUtils.class.getCanonicalName());

	
	public static boolean OptimizeJob(final JDL j, final AliEnPrincipal account, final String owner, final long masterId) throws IOException {

		logger.log(Level.FINE, "Reached the job splitter!");
	
		JobSplitter split;
		String strategy = j.gets("Split");
		switch (strategy) {
		case "parentdirectory":
			split = new SplitParse ("\\/[^\\/]*\\/?[^\\/]*$");
			break;
		case "directory":
			split = new SplitParse ("\\/[^\\/]*$");
			break;
		case "file":
			split = new SplitParse (".^");
			break;
		case "SE":
			split = new SplitSE ();
			break;
		case "Custom":
			split = new SplitCustom();
		default:
			if (strategy.startsWith("production:")) {
				split= new SplitProduction();
			} 
			else
				throw new IOException ("Error splitting: Strategy not found");

		}
		
		
		List<JDL> subjobs = split.splitJobs(j, masterId);
		
		
		
		try {
			TaskQueueUtils.insertSubJob(account, masterId, subjobs, j);
		} catch (final IOException | SQLException ioe) {
			throw new IOException("Splitting went wrong" + ioe.getMessage());
		}

		return true;
	}
	
	public static String replaceAllString(String pattern, String s, String replace) {
		Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(s);
		return m.replaceAll(replace);
	}

	public static String replaceFirstString(String pattern, String s, String replace) {
		Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(s);
		return m.replaceFirst(replace);
	}

	public static boolean patternMatches(String pattern, String s) {
		Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(s);
		if (m.find()) {
			return true;
		}
		return false;
	}
}
