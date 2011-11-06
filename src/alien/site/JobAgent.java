package alien.site;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.security.cert.X509Certificate;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobSigner;
import alien.taskQueue.JobStatus;
import alien.taskQueue.JobSubmissionException;
import alien.user.AliEnPrincipal;

/**
 * @author ron
 * @since June 5, 2011
 */
public class JobAgent extends Thread {

	private static final String tempDirPrefix = "jAliEn.JobAgent.tmp";
	private File tempDir = null;

	private static final String defaultOutputDirPrefix = "~/jalien-job-";

	private AliEnPrincipal user = null;
	private String site;

	private JDL jdl = null;
	private String sjdl = null;
	private Job job = null;
	
	private CatalogueApiUtils c_api = null;
	
	private TaskQueueApiUtils q_api = null;

	/**
	 */
	public JobAgent() {
		site = ConfigUtils.getConfig().gets("alice_close_site").trim();

	}

	@Override
	public void run() {

		while (true) {
//			Job j = q_api.getJob();
//			if (j != null)
//				handleJob(j);
//			else {
//				System.out.println("Nothing to run right now. Idling 5secs...");
//				try {
//					sleep(5000);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			}
		}
	}

	private void handleJob(final Job thejob) {
		this.job = thejob;
		try {
			sjdl = thejob.getOriginalJDL();
			System.out.println("started JA with: " + sjdl);
			jdl = new JDL(thejob.getJDL());

			if (verifiedJob()) {
				q_api.setJobStatus(thejob.queueId, JobStatus.STARTED);
				if (createTempDir())
					if (getInputFiles()) {
						if (execute())
							if (uploadOutputFiles())
								System.out.println("Job sucessfully executed.");
					} else {
						System.out.println("Could not get input files.");
						q_api.setJobStatus(thejob.queueId, JobStatus.ERROR_IB);
					}
			} else {
				q_api.setJobStatus(thejob.queueId, JobStatus.ERROR_VER);
			}
		} catch (IOException e) {
			System.err.println("Unable to get JDL from Job.");
			e.printStackTrace();
		}
	}

	private boolean verifiedJob() {
		try {
			return JobSigner.verifyJobToRun(
					new X509Certificate[] { job.userCertificate }, sjdl);

		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (SignatureException e) {
			e.printStackTrace();
		} catch (KeyStoreException e) {
			e.printStackTrace();
		} catch (JobSubmissionException e) {
			e.printStackTrace();
		}
		return false;
	}

	private boolean getInputFiles() {

		boolean gotAllInputFiles = true;
		if (jdl.getInputFiles() != null && jdl.getInputFiles().size() > 0)
			for (String slfn : jdl.getInputFiles()) {
				File localFile;
				try {
					localFile = new File(tempDir.getCanonicalFile() + "/"
							+ slfn.substring(slfn.lastIndexOf('/') + 1));

					System.out.println("Getting input file into local file: "
							+ tempDir.getCanonicalFile() + "/"
							+ slfn.substring(slfn.lastIndexOf('/') + 1));

					System.out.println("Getting input file: " + slfn);
					LFN lfn = c_api.getLFN(slfn);
					System.out.println("Getting input file lfn: " + lfn);
					List<PFN> pfns = c_api.getPFNsToRead(
							site, lfn, null, null);
					System.out.println("Getting input file pfns: " + pfns);

					for (PFN pfn : pfns) {

						List<Protocol> protocols = Transfer
								.getAccessProtocols(pfn);
						for (final Protocol protocol : protocols) {

							localFile = protocol.get(pfn, localFile);
							break;

						}
						System.out.println("Suppossed to have input file: "
								+ localFile.getCanonicalPath());
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

		final LinkedList<String> command = new LinkedList<String>();
		
		command.add(jdl.getExecutable());
		
		if (jdl.getArguments() != null)
			command.addAll(jdl.getArguments());

		System.out.println("we will run: " + command.toString());
		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(
				command);

		pBuilder.returnOutputOnExit(true);

		pBuilder.directory(tempDir);

		pBuilder.timeout(24, TimeUnit.HOURS);

		pBuilder.redirectErrorStream(true);

		try {
			final ExitStatus exitStatus;

			q_api.setJobStatus(job.queueId, JobStatus.RUNNING);

			exitStatus = pBuilder.start().waitFor();

			if (exitStatus.getExtProcExitStatus() == 0) {

				BufferedWriter out = new BufferedWriter(new FileWriter(
						tempDir.getCanonicalFile() + "/stdout"));
				out.write(exitStatus.getStdOut());
				out.close();
				BufferedWriter err = new BufferedWriter(new FileWriter(
						tempDir.getCanonicalFile() + "/stderr"));
				err.write(exitStatus.getStdErr());
				err.close();

				System.out
						.println("we ran, stdout+stderr should be there now.");
			}

			System.out.println("A local cat on stdout: "
					+ exitStatus.getStdOut());
			System.out.println("A local cat on stderr: "
					+ exitStatus.getStdErr());

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
		boolean uploadedNotAllCopies = false;
		q_api.setJobStatus(job.queueId, JobStatus.SAVING);

		String outputDir = jdl.getOutputDir();

		if (outputDir == null)
			outputDir = defaultOutputDirPrefix + job.queueId;

		System.out.println("QueueID: " + job.queueId);

		System.out.println("Full catpath of outDir is: "
				+ FileSystemUtils.getAbsolutePath(user.getName(), "~",
						outputDir));

		if (c_api.getLFN(FileSystemUtils.getAbsolutePath(
				user.getName(), null, outputDir)) != null) {
			System.err.println("OutputDir [" + outputDir + "] already exists.");
			return false;
		}

		LFN outDir = c_api
				.createCatalogueDirectory(outputDir);

		if (outDir == null) {
			System.err.println("Error creating the OutputDir [" + outputDir
					+ "].");
			uploadedAllOutFiles = false;
		}
		if (uploadedAllOutFiles) {
			for (String slfn : jdl.getOutputFiles()) {
				File localFile;
				try {
					localFile = new File(tempDir.getCanonicalFile() + "/"
							+ slfn);

					if (localFile.exists() && localFile.isFile()
							&& localFile.canRead() && localFile.length() > 0) {

						long size = localFile.length();
						if (size <= 0) {
							System.err.println("Local file has size zero: "
									+ localFile.getAbsolutePath());
						}
						String md5 = null;
						try {
							md5 = IOUtils.getMD5(localFile);
						} catch (Exception e1) {
							// ignore
						}
						if (md5 == null) {
							System.err
									.println("Could not calculate md5 checksum of the local file: "
											+ localFile.getAbsolutePath());
						}

						List<PFN> pfns = null;

						LFN lfn = null;
						lfn = c_api.getLFN(outDir.getCanonicalName() + slfn, true);

						lfn.size = size;
						lfn.md5 = md5;

						pfns = c_api.getPFNsToWrite(site,
								lfn, null, null, null, null, 0);

						if (pfns != null) {
							ArrayList<String> envelopes = new ArrayList<String>(
									pfns.size());
							for (PFN pfn : pfns) {

								List<Protocol> protocols = Transfer
										.getAccessProtocols(pfn);
								for (final Protocol protocol : protocols) {

									envelopes.add(protocol.put(pfn, localFile));
									break;

								}

							}

							// drop the following three lines once put replies
							// correctly
							// with the signed envelope
							envelopes.clear();
							for (PFN pfn : pfns)
								envelopes.add(pfn.ticket.envelope
										.getSignedEnvelope());

							List<PFN> pfnsok = c_api
									.registerEnvelopes(envelopes);
							if (!pfns.equals(pfnsok)) {
								if (pfnsok != null && pfnsok.size() > 0) {
									System.out.println("Only " + pfnsok.size()
											+ " could be uploaded");
									uploadedNotAllCopies = true;
								} else {

									System.err.println("Upload failed, sorry!");
									uploadedAllOutFiles = false;
									break;
								}
							}
						} else {
							System.out
									.println("Couldn't get write envelopes for output file");
						}
					} else {
						System.out.println("Can't upload output file "
								+ localFile.getName()
								+ ", does not exist or has zero size.");
					}

				} catch (IOException e) {
					e.printStackTrace();
					uploadedAllOutFiles = false;
				}
			}
		}
		if (uploadedNotAllCopies)
			q_api.setJobStatus(job.queueId, JobStatus.DONE_WARN);
		else if (uploadedAllOutFiles)
			q_api.setJobStatus(job.queueId, JobStatus.DONE);
		else
			q_api.setJobStatus(job.queueId, JobStatus.ERROR_SV);

		return uploadedAllOutFiles;
	}

	private boolean createTempDir() {

		String tmpDirStr = System.getProperty("java.io.tmpdir");
		if (tmpDirStr == null) {
			System.err
					.println("System temp dir config [java.io.tmpdir] does not exist.");
			return false;
		}

		File tmpDir = new File(tmpDirStr);
		if (!tmpDir.exists()) {
			boolean created = tmpDir.mkdirs();
			if (!created) {
				System.err
						.println("System temp dir [java.io.tmpdir] does not exist and can't be created.");
				return false;
			}
		}

		int suffix = (int) System.currentTimeMillis();
		int failureCount = 0;
		do {
			tempDir = new File(tmpDir, tempDirPrefix + suffix % 10000);
			suffix++;
			failureCount++;
		} while (tempDir.exists() && failureCount < 50);

		if (tempDir.exists()) {
			System.err.println("Could not create temporary directory.");
			return false;
		}
		boolean created = tempDir.mkdir();
		if (!created) {
			System.err.println("Could not create temporary directory.");
			return false;
		}

		return true;
	}

}
