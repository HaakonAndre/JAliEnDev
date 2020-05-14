package utils.crawler;

import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;
import alien.user.JAKeyStore;
import lazyj.Utils;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author anegru
 * @since Apr 25, 2020
 */
public class JobOutputMerger {

    static final Integer ARGUMENT_COUNT = 2;

    /**
     * logger
     */
    static final Logger logger = ConfigUtils.getLogger(JobOutputMerger.class.getCanonicalName());

    /**
     * JAliEnCOMMander object
     */
    static JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

    /**
     * Job argument. The directory output of the job to merge
     */
    static String outputDirectoryName;

    /**
     * Job argument. The type of the resulting merged file
     */
    static String outputFileType;


    /**
     * Merge the output of multiple jobs
     */
    public static void main(String[] args) {

        ConfigUtils.setApplicationName("JobOutputMerger");

        if (!JAKeyStore.loadKeyStore()) {
            logger.log(Level.SEVERE, "No identity found, exiting");
            return;
        }

        try {
            parseArguments(args);
        }
        catch (Exception exception) {
            logger.log(Level.SEVERE, exception.getMessage());
            return;
        }

        try {

            HashMap<String, Boolean> sesToExclude = JobSubmitter.getExcludedSEs();
            List<SE> ses = commander.c_api.getSEs(null);

            if(ses != null) {
                List<SE> filteredSEs = new ArrayList<>();
                for(SE se : ses) {
                    if(sesToExclude.get(se.seName) == null) {
                        filteredSEs.add(se);
                    }
                }

                File merged = mergeFiles(filteredSEs);
                uploadMergedFile(merged);
            }
            else {
                logger.log(Level.INFO, "Cannot get all SEs");
            }
        }
        catch (Exception exception) {
            logger.log(Level.INFO, exception.getMessage());
        }
    }

    /**
     * Parse job arguments
     * @param args
     * @throws Exception
     */
    static void parseArguments(String []args) throws Exception {

        if(args.length != ARGUMENT_COUNT) {
            throw new Exception("Number of arguments supplied is incorrect");
        }

        outputDirectoryName = args[0];
        outputFileType = args[1];

        if(!outputFileType.toLowerCase().equals("json") && !outputFileType.toLowerCase().equals("csv")) {
            throw new Exception("The type of the merged file is not supported");
        }
    }

    /**
     * Wrapper function that merges the output of all the crawling jobs run for all SEs given as parameter for the
     * specific outputFileType.
     * @param ses
     * @return
     * @throws IOException
     */
    static File mergeFiles(List<SE> ses) throws IOException {
        if(outputFileType.toLowerCase().equals("json")) {
            return mergeJSON(ses);
        }
        else {
            return mergeCSV(ses);
        }
    }

    /**
     * Merge job crawling output in JSON format
     * @param ses
     * @return
     * @throws IOException
     */
    static File mergeJSON(List<SE> ses) throws IOException {

        if(ses == null) {
            return null;
        }

        File outputFile = new File("merged_output");

        try (FileWriter fw = new FileWriter(outputFile)) {
            logger.log(Level.INFO, "Writing JSON file");

            fw.append("{");

            boolean firstSEWithData = true;

            for(SE se : ses) {
                logger.log(Level.INFO, "SE = " + se.seName);
                String seNameFolder = se.seName.replace("::", "_");

                File downloadedFile = new File(se.seName);
                commander.c_api.downloadFile(commander.getCurrentDirName() + "crawl_output/" + outputDirectoryName + "/" + seNameFolder + "/output." + outputFileType, downloadedFile);

                try {
                    String fileContents = Utils.readFile(downloadedFile.getCanonicalPath());

                    if(fileContents != null) {
                        fileContents = fileContents.substring(1, fileContents.length() - 1);

                        logger.log(Level.INFO, "Content is");

                        if(firstSEWithData) {
                            firstSEWithData = false;
                        }
                        else {
                            fileContents = "," + fileContents;
                        }

                        logger.log(Level.INFO, "Appending for SE = " + se.seName);
                        fw.append(fileContents);
                    }
                    else {
                        logger.log(Level.INFO, "File contents is null. Something went wrong with file read for SE " + se.seName);
                    }

                    if(!downloadedFile.delete()) {
                        logger.log(Level.INFO, "Downloaded file cannot be deleted for SE " + se.seName);
                    }
                }
                catch (final IOException ioe) {
                    // ignore, shouldn't be ...
                }
            }

            fw.append("}");
            fw.flush();
        }

        return outputFile;
    }

    /**
     * Merge job crawling output in CSV format
     * @param ses
     * @return
     * @throws IOException
     */
    static File mergeCSV(List<SE> ses) throws IOException {
        if(ses == null) {
            return null;
        }

        File outputFile = new File("merged_output");

        try (FileWriter fw = new FileWriter(outputFile)) {
            logger.log(Level.INFO, "Writing CSV file");

            for(SE se : ses) {
                logger.log(Level.INFO, "SE = " + se.seName);
                String seNameFolder = se.seName.replace("::", "_");

                File downloadedFile = new File(se.seName);
                commander.c_api.downloadFile(commander.getCurrentDirName() + "crawl_output/" + outputDirectoryName + "/" + seNameFolder + "/output." + outputFileType, downloadedFile);

                try {
                    String fileContents = Utils.readFile(downloadedFile.getCanonicalPath());
                    if(fileContents != null) {
                        logger.log(Level.INFO, "Appending for SE = " + se.seName);
                        fw.append(fileContents);
                    }
                    else {
                        logger.log(Level.INFO, "File contents is null. Something went wrong with file read for SE " + se.seName);
                    }

                    if(!downloadedFile.delete()) {
                        logger.log(Level.INFO, "Downloaded file cannot be deleted for SE " + se.seName);
                    }
                }
                catch (final IOException ioe) {
                    // ignore, shouldn't be ...
                }
            }

            fw.flush();
        }

        return outputFile;
    }


    /**
     * Upload the resulting merged.
     * @param merged
     * @throws IOException
     */
    static void uploadMergedFile(File merged) throws IOException {

        final String targetFileName = "/alice/cern.ch/user/a/anegru/merged." + outputFileType;
        final LFN l = commander.c_api.getLFN(targetFileName, true);

        if (l.exists) {
            if (!l.delete(true, false)) {
                throw new IOException("Could not delete previously merged file: " + targetFileName);
            }
        }

        logger.log(Level.INFO, "Uploading file");
        IOUtils.upload(merged, targetFileName, commander.getUser(), 3, null, true);
        logger.info("Uploaded " + targetFileName);
    }
}
