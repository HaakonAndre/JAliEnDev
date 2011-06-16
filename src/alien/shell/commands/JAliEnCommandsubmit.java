package alien.shell.commands;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import alien.api.taskQueue.JobSigner;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.perl.commands.AlienTime;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandsubmit extends JAliEnBaseCommand {

	public void execute() throws Exception {

		String jdl = getJDLFile(alArguments);
		if(jdl!=null)
			System.out.println("escaped JDL:" + JobSigner.signJob(jdl,JAliEnCOMMander.getUsername()));
		
			TaskQueueApiUtils.submitStatus(JobSigner.signJob(jdl,JAliEnCOMMander.getUsername()));
		
		
		

	}

	private String getJDLFile(List<String> alArguments) {
		String retJDL = null;
		System.out.println("getting jdl file: " + alArguments);
		try {
			JAliEnCommandget get = (JAliEnCommandget) JAliEnCOMMander.getCommand("get",
					new Object[] { alArguments });

			get.silent();
			get.execute();
			File out = get.getOutputFile();
			if (out != null && out.isFile() && out.canRead()) {
				FileInputStream fstream = new FileInputStream(out);

				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));

				String strLine;
				retJDL = "";
				while ((strLine = br.readLine()) != null)
					retJDL += strLine;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("JDL IS: " + retJDL);
		return retJDL;
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {

		System.out.println(AlienTime.getStamp() + "Usage: submit  ... ");
	}

	/**
	 * cat cannot run without arguments
	 * 
	 * @return <code>false</code>
	 */
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * nonimplemented command's silence trigger, submit is never silent
	 */
	public void silent() {

	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandsubmit(final ArrayList<String> alArguments) {
		super(alArguments);
	}
}
