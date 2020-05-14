package utils.crawler;

import alien.api.ServerException;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import alien.user.JAKeyStore;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author anegru
 * @since Apr 17, 2020
 */
public class JobSubmitter {

    static final Integer ARGUMENT_COUNT = 6;

    /**
     * logger
     */
    static final Logger logger = ConfigUtils.getLogger(JobSubmitter.class.getCanonicalName());

    /**
     * JAlienCOMMander object
     */
    static JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

    /**
     * The directory name of the output file
     */
    static String outputDirectoryName;

    /**
     * The initial number of files to crawl from each SE
     */
    static String initialFileCount;

    /**
     * The maximum time allowed for a job to stay in the running state
     */
    static String maxRunningTimeSeconds;

    /**
     * The maximum time allowed for a job to stay in the waiting queue
     */
    static String maxWaitingTimeSeconds;

    /**
     * The job output file type
     */
    static String outputFileType;

    /**
     * The number of threads used when crawling files
     */
    static String crawlingThreadCount;

    /**
     * Entry point where the Submit job starts
     * Submit jobs that extract random PFNs from all SEs in order to detect corrupt files.
     * @param args
     */
    public static void main(String[] args) {
        try {
            ConfigUtils.setApplicationName("SubmitJobs");

            if (!JAKeyStore.loadKeyStore()) {
                logger.log(Level.SEVERE, "No identity found, exiting");
                return;
            }

            parseArguments(args);

            HashMap<Long, Boolean> mapJobIdToIsFinished = new HashMap<>();
            HashMap<String, Boolean> sesToExclude = getExcludedSEs();
            List<SE> ses = commander.c_api.getSEs(null);

            if(ses == null) {
                logger.log(Level.INFO, "Cannot retreive SEs.");
                return;
            }

            //submitting jobs to all available SEs
            for (SE se : ses) {
                if(sesToExclude.get(se.seName) != null) {
                    logger.log(Level.INFO, "SE " + se.seName + " is excluded from search. Skipping.");
                    continue;
                }

                logger.log(Level.INFO, "Crawling " + se.seName);
                JDL jdl = getJDLCrawlSE(se);
                try {
                    long jobId = commander.q_api.submitJob(jdl);
                    mapJobIdToIsFinished.put(jobId, false);
                }
                catch (ServerException e) {
                    logger.log(Level.INFO, "Submitting job to SE " + se.seName + " failed");
                }
            }

            //check if all jobs have finished
            int finishedJobs = 0;
            int submittedJobsCount = mapJobIdToIsFinished.keySet().size();

            while (finishedJobs != submittedJobsCount) {
                for (Long jobId : mapJobIdToIsFinished.keySet()) {
                    Job job = commander.q_api.getJob(jobId);
                    if(!mapJobIdToIsFinished.get(jobId) && (job.isFinalState() || job.status() == JobStatus.KILLED)) {
                        mapJobIdToIsFinished.put(jobId, true);
                        finishedJobs += 1;
                    }
                }

                Thread.sleep(5000);
            }

            JDL jdl = getJDLMergeCrawlOutput();
            try {
                commander.q_api.submitJob(jdl);
            }
            catch (ServerException e) {
                logger.log(Level.INFO, "Submitting job to merge crawling output failed");
            }

        }
        catch(Exception e) {
            logger.log(Level.INFO, e.getMessage());
        }
    }

    /**
     * Parse job arguments
     * @param args
     * @throws Exception
     */
    public static void parseArguments(String []args) throws Exception {

        if(args.length != ARGUMENT_COUNT) {
            throw new Exception("Number of arguments supplied is incorrect");
        }

        outputDirectoryName = args[0];
        initialFileCount = args[1];
        maxRunningTimeSeconds = args[2];
        maxWaitingTimeSeconds = args[3];
        outputFileType = args[4];
        crawlingThreadCount = args[5];
    }

    /**
     * Returns a hashmap whose keys are SE names that must be excluded from search
     * It returns a hashmap to provide efficient lookup for SE names.
     * @return
     */
    public static HashMap<String, Boolean> getExcludedSEs() {
        HashMap<String, Boolean> excludedSEs = new HashMap<>();
        excludedSEs.put("NO_SE", true);
        return excludedSEs;
    }

    /**
     * Get JDL to be used to crawl files from a specific SE
     * @param se
     * @return JDL
     */
    public static JDL getJDLCrawlSE(SE se) {
        JDL jdl = new JDL();
        jdl.append("JobTag", "FileCrawler");
        jdl.set("OutputDir", commander.getCurrentDirName() + "/crawl_output/" + outputDirectoryName + "/" + se.seName.replace("::", "_"));
        jdl.append("InputFile", "LF:" + commander.getCurrentDirName() + "/" + "alien-users.jar");
        jdl.set("Arguments", outputDirectoryName + " " + se.seNumber + " " + initialFileCount + " " + maxRunningTimeSeconds + " " + outputFileType + " " + crawlingThreadCount);
        jdl.set("Executable", commander.getCurrentDirName() + "/" + "crawl.sh");
        jdl.set("MaxWaitingTime", maxWaitingTimeSeconds + "s");
        jdl.set("TTL", 3600);
        jdl.set("Price", 1);
        jdl.append("JDLVariables", "FilesToCheck");
        jdl.append("Output", "crawler.log");
        jdl.append("Workdirectorysize", "11000MB");
        jdl.set("Requirements", getSiteJDLRequirement(se.seName));
        return jdl;
    }

    /**
     * Generate JDL requirement for closest site to storage element
     * @param se
     */
    private static StringBuilder getSiteJDLRequirement(String se) {
        final Set<String> siteNames = GetCEs.getCloseSites(se);

        final Set<String> matchingCEs = new LinkedHashSet<>();

        for (final String siteName : siteNames)
            for (final String ce : GetCEs.getCEs(siteName))
                matchingCEs.add(siteName + "::" + ce);

        final StringBuilder requirements = new StringBuilder();

        for (final String ce : matchingCEs) {
            if (requirements.length() > 0)
                requirements.append(" || ");

            requirements.append("(other.CE==\"ALICE::").append(ce).append("\")");
        }

        return requirements;
    }

    /**
     * Get JDL to be used to merge the output of multiple crawling jobs
     * @return JDL
     */
    public static JDL getJDLMergeCrawlOutput() {
        JDL jdl = new JDL();
        jdl.append("JobTag", "OutputMerger");
        jdl.set("OutputDir", commander.getCurrentDirName() + "/merge_log/" + outputDirectoryName);
        jdl.append("InputFile", "LF:" + commander.getCurrentDirName() + "/" + "alien-users.jar");
        jdl.set("Arguments", outputDirectoryName + " " + outputFileType);
        jdl.set("Executable", commander.getCurrentDirName() + "/" + "merge.sh");
        jdl.set("TTL", 3600);
        jdl.set("Price", 1);
        jdl.append("JDLVariables", "FilesToCheck");
        jdl.append("Output", "merge.log");
        jdl.append("Workdirectorysize", "11000MB");
        return jdl;
    }
}
