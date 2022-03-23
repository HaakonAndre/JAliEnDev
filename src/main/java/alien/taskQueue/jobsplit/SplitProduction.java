package alien.taskQueue.jobsplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.taskQueue.JDL;

public class SplitProduction extends JobSplitter {
	
	public List<JDL> splitJobs(final JDL j, long masterId) throws IOException {
		String pattern = "^production:(.+)-(.+)";
		Pattern r = Pattern.compile(pattern);

		Matcher m = r.matcher(j.gets("split"));

		int start = 0;
		int end = 0;
		try {
			start = Integer.parseInt(m.group(1));
			end = Integer.parseInt(m.group(2));
		}
		catch (Exception e){
			throw new IOException("Error splitting production: " + e.getMessage());
		}


		List<JDL> jdls = new ArrayList<JDL>();
		for (int i = start; i <= end; i++) {
			JDL tmpJdl = prepareSubJobJDL(j, masterId, 0, null);
			jdls.add(tmpJdl);
		}

		return jdls;
		
	}

}
