package alien.site;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.shell.commands.JAliEnCOMMander;
import alien.taskQueue.JDL;
import alien.ui.api.CatalogueApiUtils;

/**
 * @author ron
 * @since June 5, 2011
 */
public class JobAgent extends Thread {

	private LinkedList<String> command = null;
	private LinkedList<String> inputFiles = null;
	private LinkedList<String> outputFiles = null;
	private File tempDir = null;

	private JDL jdl = null;

	JobAgent(JDL jdl) {
		this.jdl = jdl;
	}

	public void run() {
		parseJDL();
		if (createTempDir())
			if (getInputFiles()){
				if (execute())
					if (uploadOutputFiles())
						System.out.println("Job sucessfully executed.");}
			else
				System.out.println("Could not get input files.");

	}

	private void parseJDL() {

		String executeable = (String) jdl.get("Executeable");
		String arguments = (String) jdl.get("Arguments");
		command = new LinkedList<String>();
		command.add(executeable);
		command.add(arguments);

		inputFiles = new LinkedList<String>();
		outputFiles = new LinkedList<String>();
		inputFiles.add((String) jdl.get("InputFiles"));
		outputFiles.add((String) jdl.get("OutputFiles"));

	}

	private boolean getInputFiles() {

		boolean gotAllInputFiles = true;

		for (String slfn : inputFiles) {
			File localFile;
			try {
				localFile = new File(tempDir.getCanonicalFile() + "/"
						+ slfn.substring(slfn.lastIndexOf('/') + 1));
				
				System.out.println("Getting input file into local file: " + tempDir.getCanonicalFile() + "/"
						+ slfn.substring(slfn.lastIndexOf('/') + 1));

				System.out.println("Getting input file: " + slfn);
				LFN lfn = CatalogueApiUtils.getLFN(slfn);
				System.out.println("Getting input file lfn: " + lfn);
				List<PFN> pfns = CatalogueApiUtils.getPFNsToRead(
						JAliEnCOMMander.getUser(), JAliEnCOMMander.getSite(),
						lfn, null, null);
				System.out.println("Getting input file pfns: " + pfns);

				for (PFN pfn : pfns) {

					List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
					for (final Protocol protocol : protocols) {

						localFile = protocol.get(pfn, localFile);
						break;

					}
					System.out.println("Suppossed to have input file: " + localFile.getCanonicalPath());
				}
				if (!localFile.exists())
					gotAllInputFiles = false;
			} catch (IOException e) {
				e.printStackTrace();
				gotAllInputFiles = false;
			}
		}
		return gotAllInputFiles;

	}

	private boolean execute() {

		boolean ran = true;

		System.out.println("we will run: " + command.toString());
		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
				command);

		pBuilder.returnOutputOnExit(true);

		pBuilder.timeout(24, TimeUnit.HOURS);

		pBuilder.redirectErrorStream(true);

		try {
			final ExitStatus exitStatus;

			exitStatus = pBuilder.start().waitFor();

			System.out.println("ran, stdout: " + exitStatus.getStdOut());
			System.out.println("ran, stderr: " + exitStatus.getStdErr());

			
			if (exitStatus.getExtProcExitStatus() != 0) {

				BufferedWriter out = new BufferedWriter(new FileWriter(
						tempDir.getCanonicalFile() + "/stdout"));
				out.write(exitStatus.getStdOut());
				out.close();
				BufferedWriter err = new BufferedWriter(new FileWriter(
						tempDir.getCanonicalFile() + "/stderr"));
				err.write(exitStatus.getStdErr());
				err.close();
			}
		} catch (final InterruptedException ie) {
			System.err
					.println("Interrupted while waiting for the following command to finish : "
							+ command.toString());
			ran = false;
		} catch (IOException e) {
			ran = false;
		}
		return ran;
	}

	private boolean uploadOutputFiles() {

		boolean uploadedAllOutFiles = true;

		for (String slfn : outputFiles) {
			File localFile;
			try {
				localFile = new File(tempDir.getCanonicalFile() + "/"
						+ slfn.substring(slfn.lastIndexOf('/') + 1));

				LFN lfn = CatalogueApiUtils.getLFN(slfn);
				List<PFN> pfns = CatalogueApiUtils.getPFNsToRead(
						JAliEnCOMMander.getUser(), JAliEnCOMMander.getSite(),
						lfn, null, null);
				
				ArrayList<String> envelopes = new ArrayList<String>(pfns.size());
				for (PFN pfn : pfns) {

					List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
					for (final Protocol protocol : protocols) {

						envelopes.add(protocol.put(pfn, localFile));
						break;

					}
				}
				
				// drop the following three lines once put replies correctly with the signed envelope
				envelopes.clear();
				for (PFN pfn : pfns)
					envelopes.add(pfn.ticket.envelope.getSignedEnvelope()); 
				
				
				List<PFN> pfnsok = 	CatalogueApiUtils.registerEnvelopes(JAliEnCOMMander.getUser(), envelopes);
				if(!pfns.equals(pfnsok)){
				 if(pfnsok!=null && pfnsok.size()>0)
					System.out.println("Only " + pfnsok.size()+ " could be uploaded");
				else 
					System.err.println("Upload failed, sorry!");
				 uploadedAllOutFiles = false;
				}
			} catch (IOException e) {
				uploadedAllOutFiles = false;
			}
		}
		return uploadedAllOutFiles;
	}

	private boolean createTempDir() {
		File dir = new File("/tmp");
		try {
			tempDir = File.createTempFile("JavaTemp", ".javatemp", dir);
		} catch (IOException ioe) {
			System.err.println("Could not create temporary directory.");
			return false;
		}
		return true;
	}

}
