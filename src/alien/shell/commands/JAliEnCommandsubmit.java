package alien.shell.commands;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import lazyj.Utils;
import alien.api.taskQueue.SubmitJob;
import alien.shell.ShellColor;
import alien.taskQueue.JobSubmissionException;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandsubmit extends JAliEnCommandcat {

	@Override
	public void run() {
		
		int queueId = 0;
		if (!isSilent())
			out.printOutln("Submitting " + alArguments.get(0));
		
		File fout = catFile();
		
		if (fout!=null && fout.exists() && fout.isFile() && fout.canRead()) {
			final String content  =  Utils.readFile(fout.getAbsolutePath());
			if (content!=null)
				try {
					queueId = commander.q_api.submitJob(content);
					if(queueId>0){
						if (!isSilent())
							out.printOutln("Your new job ID is " + ShellColor.blue() + queueId + ShellColor.reset());
					}else{
						if (!isSilent())
							out.printErrln("Error submitting " + alArguments.get(0));
					}
				} catch (JobSubmissionException e) {
					e.printStackTrace();
					if (!isSilent())
						out.printErrln("Error submitting " + alArguments.get(0));
				}
			else
				if (!isSilent())
					out.printErrln("Could not read the contents of "+fout.getAbsolutePath());
		} 
		else
			if (!isSilent())
				out.printErrln("Not able to get the file " + alArguments.get(0));
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("submit","<URL>"));
		out.printOutln();
		out.printOutln(helpParameter("<URL> => <LFN>"));
		out.printOutln(helpParameter("<URL> => file:///<local path>"));
		out.printOutln();
	}


	/**
	 * Constructor needed for the command factory in commander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandsubmit(JAliEnCOMMander commander, UIPrintWriter out,
			final ArrayList<String> alArguments) {
		super(commander, out, alArguments);
	}
}
