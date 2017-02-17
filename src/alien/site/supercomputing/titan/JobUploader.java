package alien.site.supercomputing.titan;

import alien.site.OutputEntry;

import apmon.ApMon;
import apmon.ApMonException;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

import lia.util.Utils;

import alien.api.JBoxServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.ParsedOutput;
import alien.site.TitanJobService;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;

public class JobUploader extends Thread{
	TitanJobStatus js;
	private String dbname;
	private Long queueId;
	private JDL jdl;

	private String jobWorkdir;
	private JobStatus jobStatus;
	FileDownloadController fdc;

	private String username;

	public static String ce;
	public static String hostName;
	public static String defaultOutputDirPrefix; 

	static transient final Logger logger = ConfigUtils.getLogger(TitanJobService.class.getCanonicalName());
	static transient final Monitor monitor = MonitorFactory.getMonitor(TitanJobService.class.getCanonicalName());
	static transient final ApMon apmon = MonitorFactory.getApMonSender();
	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private final CatalogueApiUtils c_api = new CatalogueApiUtils(commander);

	public JobUploader(TitanJobStatus js){
		fdc = FileDownloadController.getInstance();
		this.js = js;
		if(js.executionCode!=0)
			jobStatus = JobStatus.ERROR_E;
		else if(js.validationCode!=0)
			jobStatus = JobStatus.ERROR_V;
		else
			jobStatus = JobStatus.DONE;
		dbname = js.batch.dbName;
		username = "";
	}

	public void run(){
		queueId = js.queueId;
		System.err.println(String.format("Uploading job: %d", queueId));
		jobWorkdir = js.jobFolder;
		File tempDir = new File(js.jobFolder);

		String jdl_content = null;
		try{
			byte[] encoded = Files.readAllBytes(Paths.get(js.jobFolder + "/jdl"));
			jdl_content = new String(encoded, Charset.defaultCharset());
		}
		catch(IOException e){
			System.err.println("Unable to read JDL file: " + e.getMessage());
		}
		if( jdl_content!=null ){
			jdl = null;
			try{
				jdl = new JDL(Job.sanitizeJDL(jdl_content));
			}
			catch(IOException e){
				System.err.println("Unable to parse JDL: " + e.getMessage());
			}
			if(jdl!=null){
				if(js.executionCode!=0) {
					changeStatus(queueId, JobStatus.ERROR_E);
					Vector<String> varnames = new Vector<>();
					varnames.add("host");
					varnames.add("statusID");
					varnames.add("jobID");
					Vector<Object> varvalues = new Vector<>();
					varvalues.add(hostName);
					varvalues.add("-3");
					varvalues.add(queueId);
					try{
						apmon.sendParameters(ce+"_Jobs", String.format("%d",queueId), 2, varnames, varvalues);
					}
					catch(ApMonException e){}
					catch(UnknownHostException e){}
					catch(SocketException e){}
					catch(IOException e){}
				}
				else if(js.validationCode!=0){
					changeStatus(queueId, JobStatus.ERROR_V);
					Vector<String> varnames = new Vector<>();
					varnames.add("host");
					varnames.add("statusID");
					varnames.add("jobID");
					Vector<Object> varvalues = new Vector<>();
					varvalues.add(hostName);
					varvalues.add("-10");
					varvalues.add(queueId);
					try{
						apmon.sendParameters(ce+"_Jobs", String.format("%d",queueId), 2, varnames, varvalues);
					}
					catch(ApMonException e){}
					catch(UnknownHostException e){}
					catch(SocketException e){}
					catch(IOException e){}
				}
				else{
					changeStatus(queueId, JobStatus.SAVING);
					Vector<String> varnames = new Vector<>();
					varnames.add("host");
					varnames.add("statusID");
					varnames.add("jobID");
					Vector<Object> varvalues = new Vector<>();
					varvalues.add(hostName);
					varvalues.add("11");
					varvalues.add(queueId);
					try{
						apmon.sendParameters(ce+"_Jobs", String.format("%d",queueId), 2, varnames, varvalues);
					}
					catch(ApMonException e){}
					catch(UnknownHostException e){}
					catch(SocketException e){}
					catch(IOException e){}
				}
				uploadOutputFiles();	// upload data
				cleanup();
				System.err.println(String.format("Upload job %d finished", queueId));

				int i = 50;
				while(i-->0){
					try{
						Connection connection = DriverManager.getConnection(dbname);
						Statement statement = connection.createStatement();
						statement.executeUpdate(String.format("UPDATE alien_jobs SET status='I' WHERE rank=%d", js.rank));
						connection.close();
					}
					catch(SQLException e){
						System.err.println("Update job state to I failed: " +
								e.getMessage());
						try{
							Thread.sleep(2000);
						}
						catch(InterruptedException ei){
							System.err.println("Sleep in DispatchSSLMTClient.getInstance has been interrupted");
						}
						continue;
					}
					return;
				}
			}
		}
	}

	public void setDbName(String dbn){
		dbname = dbn;
	}

	private void cleanup() {
		System.out.println("Cleaning up after execution...Removing sandbox: " + jobWorkdir);
		// Remove sandbox, TODO: use Java builtin
		//Utils.getOutput("rm -rf " + jobWorkdir);
		Utils.getOutput("cp -r " + jobWorkdir + " /lustre/atlas/scratch/psvirin/csc108/cleanup_folder");
		/* RES_WORKDIR_SIZE = ZERO;
		RES_VMEM = ZERO;
		RES_RMEM = ZERO;
		RES_VMEMMAX = ZERO;
		RES_RMEMMAX = ZERO;
		RES_MEMUSAGE = ZERO;
		RES_CPUTIME = ZERO;
		RES_CPUUSAGE = ZERO;
		RES_RESOURCEUSAGE = "";
		RES_RUNTIME = Long.valueOf(0);
		RES_FRUNTIME = "";
		*/
	}

	private boolean uploadOutputFiles() {
		boolean uploadedAllOutFiles = true;
		boolean uploadedNotAllCopies = false;

		commander.q_api.putJobLog(queueId, "trace", "Going to uploadOutputFiles");

		// EXPERIMENTAL
		final String outputDir = getJobOutputDir();
		//final String outputDir = getJobOutputDir() + "/"  + queueId;

		System.out.println("queueId: " + queueId);
		System.out.println("outputDir: " + outputDir);

		if (c_api.getLFN(outputDir) != null) {
			System.err.println("OutputDir [" + outputDir + "] already exists.");
			changeStatus(queueId, JobStatus.ERROR_SV);

			Vector<String> varnames = new Vector<>();
			varnames.add("host");
			varnames.add("statusID");
			varnames.add("jobID");
			Vector<Object> varvalues = new Vector<>();
			varvalues.add(hostName);
			varvalues.add("-9");
			varvalues.add(queueId);
			try{
				apmon.sendParameters(ce+"_Jobs", String.format("%d",queueId), 2, varnames, varvalues);
				apmon.sendParameters("TaskQueue_Jobs_ALICE", 
						String.format("%d",queueId), 
						3, varnames, varvalues);
			}
			catch(ApMonException e){}
			catch(UnknownHostException e){}
			catch(SocketException e){}
			catch(IOException e){}
			return false;
		}

		final LFN outDir = c_api.createCatalogueDirectory(outputDir);

		if (outDir == null) {
			System.err.println("Error creating the OutputDir [" + outputDir + "].");
			uploadedAllOutFiles = false;
		} else {
			String tag = "Output";
			if (jobStatus == JobStatus.ERROR_E)
				tag = "OutputErrorE";

			final ParsedOutput filesTable = new ParsedOutput(queueId, jdl, jobWorkdir, tag);

			for (final OutputEntry entry : filesTable.getEntries()) {
				File localFile;
				try {
					if (entry.isArchive())
						entry.createZip(jobWorkdir);

					localFile = new File(jobWorkdir + "/" + entry.getName());
					System.out.println("Processing output file: " + localFile);

					// EXPERIMENTAL
					System.err.println("===================");
					System.err.println("Filename: " + localFile.getName());
					System.err.println(String.format("File exists: %b", localFile.exists()));
					System.err.println(String.format("File is file: %b", localFile.isFile()));
					System.err.println(String.format("File readable: %b", localFile.canRead()));
					System.err.println(String.format("File length: %d", localFile.length()));

					if (localFile.exists() && localFile.isFile() && localFile.canRead() && localFile.length() > 0) {

						final long size = localFile.length();
						if (size <= 0)
							System.err.println("Local file has size zero: " + localFile.getAbsolutePath());
						String md5 = null;
						try {
							md5 = IOUtils.getMD5(localFile);
						} catch (final Exception e1) {
							// ignore
						}
						if (md5 == null)
							System.err.println("Could not calculate md5 checksum of the local file: " + localFile.getAbsolutePath());

						final LFN lfn = c_api.getLFN(outDir.getCanonicalName() + "/" + entry.getName(), true);
						lfn.size = size;
						lfn.md5 = md5;
						lfn.jobid = queueId;
						lfn.type = 'f';
						final GUID guid = GUIDUtils.createGuid(localFile, commander.getUser());
						lfn.guid = guid.guid;
						final ArrayList<String> exses = entry.getSEsDeprioritized();

						final List<PFN> pfns = c_api.getPFNsToWrite(lfn, guid, entry.getSEsPrioritized(), exses, entry.getQoS());

						System.out.println("LFN :" + lfn + "\npfns: " + pfns);

						commander.q_api.putJobLog(queueId, "trace", "Uploading: " + lfn.getName());

						if (pfns != null && !pfns.isEmpty()) {
							final ArrayList<String> envelopes = new ArrayList<>(pfns.size());
							for (final PFN pfn : pfns) {
								final List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
								for (final Protocol protocol : protocols) {
									envelopes.add(protocol.put(pfn, localFile));
									break;
								}
							}

							// drop the following three lines once put replies
							// correctly
							// with the signed envelope
							envelopes.clear();
							for (final PFN pfn : pfns)
								envelopes.add(pfn.ticket.envelope.getSignedEnvelope());

							final List<PFN> pfnsok = c_api.registerEnvelopes(envelopes);
							if (!pfns.equals(pfnsok))
								if (pfnsok != null && pfnsok.size() > 0) {
									System.out.println("Only " + pfnsok.size() + " could be uploaded");
									uploadedNotAllCopies = true;
								} else {
									System.err.println("Upload failed, sorry!");
									uploadedAllOutFiles = false;
									break;
								}
						} else
							System.out.println("Couldn't get write envelopes for output file");
					} else
						System.out.println("Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");

				} catch (final IOException e) {
					e.printStackTrace();
					uploadedAllOutFiles = false;
				}
			}
		}

		if (jobStatus != JobStatus.ERROR_E && jobStatus != JobStatus.ERROR_V)
			if (uploadedNotAllCopies){
				changeStatus(queueId, JobStatus.DONE_WARN);
				Vector<String> varnames = new Vector<>();
				varnames.add("host");
				varnames.add("statusID");
				varnames.add("jobID");
				Vector<Object> varvalues = new Vector<>();
				varvalues.add(hostName);
				varvalues.add("16");
				varvalues.add(queueId);
				try{
					apmon.sendParameters(ce+"_Jobs", String.format("%d",queueId), 2, varnames, varvalues);
				}
				catch(ApMonException e){}
				catch(UnknownHostException e){}
				catch(SocketException e){}
				catch(IOException e){}
			}
			else if (uploadedAllOutFiles){
				changeStatus(queueId, JobStatus.DONE);
				Vector<String> varnames = new Vector<>();
				varnames.add("host");
				varnames.add("statusID");
				varnames.add("jobID");
				Vector<Object> varvalues = new Vector<>();
				varvalues.add(hostName);
				varvalues.add("15");
				varvalues.add(queueId);
				try{
					apmon.sendParameters(ce+"_Jobs", String.format("%d",queueId), 2, varnames, varvalues);
					apmon.sendParameters("TaskQueue_Jobs_ALICE", 
							String.format("%d",queueId), 
							3, varnames, varvalues);
				}
				catch(ApMonException e){}
				catch(UnknownHostException e){}
				catch(SocketException e){}
				catch(IOException e){}
			}
			else{
				changeStatus(queueId, JobStatus.ERROR_SV);
				Vector<String> varnames = new Vector<>();
				varnames.add("host");
				varnames.add("statusID");
				varnames.add("jobID");
				Vector<Object> varvalues = new Vector<>();
				varvalues.add(hostName);
				varvalues.add("-9");
				varvalues.add(queueId);
				try{
					apmon.sendParameters(ce+"_Jobs", String.format("%d",queueId), 2, varnames, varvalues);
				}
				catch(ApMonException e){}
				catch(UnknownHostException e){}
				catch(SocketException e){}
				catch(IOException e){}
			}

		return uploadedAllOutFiles;
	}

	/**
	 * @return job output dir (as indicated in the JDL if OK, or the recycle path if not)
	 */
	public String getJobOutputDir() {
		String outputDir = jdl.getOutputDir();

		if (jobStatus == JobStatus.ERROR_V || jobStatus == JobStatus.ERROR_E)
			outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + "recycle/" + defaultOutputDirPrefix + queueId);
		else if (outputDir == null)
			outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + defaultOutputDirPrefix + queueId);

		return outputDir;
	}

	/**
	 * @param newStatus
	 */
	public void changeStatus(final Long queueId, final JobStatus newStatus) {
		// if final status with saved files, we set the path
		if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN || newStatus == JobStatus.ERROR_E || newStatus == JobStatus.ERROR_V) {
			final HashMap<String, Object> extrafields = new HashMap<>();
			extrafields.put("path", getJobOutputDir());

			TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
		} else if (newStatus == JobStatus.RUNNING) {
			final HashMap<String, Object> extrafields = new HashMap<>();
			extrafields.put("spyurl", hostName + ":" + JBoxServer.getPort());
			extrafields.put("node", hostName);
			extrafields.put("exechost", hostName);

			TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
		} else
			TaskQueueApiUtils.setJobStatus(queueId, newStatus);

		jobStatus = newStatus;

		return;
	}

}
