package alien.shell.commands;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import alien.taskQueue.JobSubmissionException;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandsubmit extends JAliEnBaseCommand {

	public void run(){

		String jdl = getJDLFile(alArguments);
		if (!jdl.equals(""))

			jdl = analyzeAndPrepareJDL(jdl);

		if (jdl != null) {

//			System.out.println("signed JDL:\n"
//					+ JobSigner.signJob(JAKeyStore.clientCert, "User.cert",
//							JAKeyStore.pass, commander.getUsername(), jdl));

			
			try {
				int jobID = commander.q_api.submitJob(jdl);
				out.printOutln("[" + jobID + "] Job successfully submitted.");
			} catch (JobSubmissionException e) {
				out.printErrln("Submission failed:");
				out.printErrln(e.getMessage());
			}

		}
		
		out.printErrln("--- not implemented yet ---");


	}

	private String analyzeAndPrepareJDL(String jdl) {

		return jdl + "\nUser = {\"" + commander.getUsername() + "\"};\n";
	}

	private String getJDLFile(List<String> alArguments) {
		String file = "";
		out.printOutln("Submitting JDL: " + alArguments);
		try {

			JAliEnCommandget get = (JAliEnCommandget) JAliEnCOMMander
					.getCommand("get", new Object[] { commander, out,
							alArguments });
			get.silent();
			get.run();
			File fout = get.getOutputFile();
			if (fout != null && fout.isFile() && fout.canRead()) {
				FileInputStream fstream = new FileInputStream(fout);

				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String line;
				while ((line = br.readLine()) != null) {
					file += line + "\n";
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	//	out.printOutln("JDL IS: |" + file+"|");
		return file;
	}

	/**
	 * printout the help info
	 */
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("submit","<URL>"));
		out.printOutln();
		out.printOutln(helpParameter("<URL> => alien://<lfn>"));
		out.printOutln(helpParameter("<URL> => /alien<lfn>"));
		out.printOutln(helpParameter("<URL> => <local path>"));
		out.printOutln();
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
      //ignore
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
