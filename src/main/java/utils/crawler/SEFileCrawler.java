package utils.crawler;

import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.protocols.Factory;
import alien.io.protocols.Xrootd;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.user.JAKeyStore;
import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;

/**
 * @author anegru
 * @since Apr 17, 2020
 */
public class SEFileCrawler {

    private static final String OUTPUT_FORMAT_JSON = "json";
    private static final String OUTPUT_FORMAT_CSV = "csv";
    private static final String OUTPUT_FILE_NAME = "output";
    private static final Integer OUTPUT_LFN_REPLICA_COUNT = 3;
    private static final Integer ARGUMENT_COUNT = 5;

    /**
     * logger
     */
    static final Logger logger = ConfigUtils.getLogger(SEFileCrawler.class.getCanonicalName());

    /**
     * JAliEnCOMMander object
     */
    static final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

    /**
     * Xrootd for download operation
     */
    static final Xrootd xrootd = (Xrootd) Factory.xrootd.clone();

    /**
     * Map GUID to a PFNCrawled object. Contains data is is written to output.
     */
    static Map<String, PFNCrawled> mapGuidToPFN = new HashMap<>();

    /**
     * Storage element object
     */
    static SE se;

    /**
     * The initial number of files to crawl from the SE
     */
    static int fileCount = 0;

    /**
     * The maximum number of seconds this job must spend crawling files.
     */
    static int maxRunningTimeSeconds = 0;

    /**
     * Output file format type. (eg 'json')
     */
    static String outputFileType;

    /**
     * Output directory name
     */
    static String outputDirectoryName;

    /**
     * Command line arguments:
     *
     * integer seNumber
     * integer fileCount
     * integer maxRunningTimeSeconds
     * String outputType
     *
     * Extract random PFNs from storage element with number seNumber. The job can last for at most
     * maxRunningTimeSeconds seconds. Initially extract fileCount elements. If the duration is not exceeded
     * and there is enough time for more files to be extracted, the fileCount is doubled at each iteration.
     * If outputType = "json", the output of the job is written in JSON format, otherwise CSV format is used
     *
     * @param args
     */
    public static void main(String[] args) {
        logger.log(Level.INFO, "Start crawling");

        ConfigUtils.setApplicationName("ChecksumFileCrawler");

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

        long globalStartTimestamp = System.currentTimeMillis();
        long globalEndTimestamp = globalStartTimestamp + maxRunningTimeSeconds * 1000;
        long currentTimestamp = globalStartTimestamp;

        while (currentTimestamp < globalEndTimestamp) {

            long chunkStartTimestamp = System.currentTimeMillis();

            crawlPFNs(fileCount);

            long chunkEndTimestamp = System.currentTimeMillis();
            long chunkDuration = chunkEndTimestamp - chunkStartTimestamp;
            long durationPerFile = chunkDuration / fileCount;
            currentTimestamp = System.currentTimeMillis();


            if(currentTimestamp + durationPerFile * fileCount < globalEndTimestamp) {
                logger.log(Level.INFO, "Exiting. The number of files to be crawled is set to " + fileCount + " but it will most likely not be able to complete before " + globalEndTimestamp);
                break;
            }

            if(currentTimestamp + durationPerFile * (fileCount * 2) < globalEndTimestamp) {
                logger.log(Level.INFO, "The number of files to extract in the next rount has ben doubled to " + fileCount);
                fileCount *= 2;
            }
        }

        try {
            writeJobOutputToFile(outputFileType);
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    /**
     * Parse job arguments
     * @param args
     * @throws Exception
     */
    public static void parseArguments(String []args) throws Exception {

        if (args.length != ARGUMENT_COUNT) {
            throw new Exception("Number of arguments supplied is incorrect");
        }

        outputDirectoryName = args[0];

        int seNumber = Integer.parseInt(args[1]);

        fileCount = Integer.parseInt(args[2]);
        maxRunningTimeSeconds = Integer.parseInt(args[3]);
        outputFileType = args[4];

        if (maxRunningTimeSeconds < 0) {
            logger.log(Level.INFO, "Job duration must be a positive integer");
            return;
        }

        se = SEUtils.getSE(seNumber);

        if (se == null) {
            throw new Exception("Storage element with number " + seNumber + " does not exist");
        }
    }

    /**
     * Crawl fileCount random files from the SE
     * @param fileCount
     */
    public static void crawlPFNs(int fileCount) {

        Collection<PFN> randomPFNs = commander.c_api.getRandomPFNsFromSE(se.seNumber, fileCount);

        for (PFN currentPFN : randomPFNs) {
            try {
                PFN pfnToRead = getPFNWithAccessToken(currentPFN);
                boolean checksumValid = validateChecksum(pfnToRead);
                updateJobOutput(pfnToRead, checksumValid);
            }
            catch (Exception exception) {
                logger.log(Level.INFO,  exception.getMessage());
            }
        }
    }

    /**
     * Fill access token of PFN so that it can be read
     * @param pfn
     * @return PFN
     * @throws Exception
     */
    public static PFN getPFNWithAccessToken(PFN pfn) throws Exception {
        GUID guid = pfn.getGuid();

        if(guid == null) {
            throw new Exception("PFN " + pfn.pfn + " has a null GUID");
        }

        List<String> ses = new ArrayList<>();
        List<String> exses = new ArrayList<>();
        ses.add(se.seName);

        Collection<PFN> pfnsToRead = commander.c_api.getPFNsToRead(guid, ses, exses);

        if(pfnsToRead == null) {
            throw new Exception("Cannot get PFNs to read for " + pfn.pfn);
        }

        Iterator<PFN> pfnIterator = pfnsToRead.iterator();

        if (!pfnIterator.hasNext()) {
            throw new Exception("Cannot get PFNs to read for " + pfn.pfn);
        }

        return pfnIterator.next();
    }

    /**
     * The PFN must have an access token.
     * Download the file, recompute the checksum and compare it with the checksum from the catalogue.
     * @param pfnToRead
     * @return boolean
     * @throws Exception
     */
    public static boolean validateChecksum(PFN pfnToRead) throws Exception {

        File downloadedFile = xrootd.get(pfnToRead, null);
        GUID guid = pfnToRead.getGuid();

        if(downloadedFile == null) {
            throw new Exception("Cannot download " + pfnToRead.pfn);
        }

        if(guid == null) {
            throw new Exception("PFN " + pfnToRead.pfn + " has a null GUID");
        }

        String md5RecomputedAfterDownload = IOUtils.getMD5(downloadedFile);
        String md5FromCatalogue = guid.md5;

        if(!downloadedFile.delete()) {
            logger.log(Level.INFO, "Cannot delete " + downloadedFile.getName());
        }

        return md5RecomputedAfterDownload.equals(md5FromCatalogue);
    }

    public static void updateJobOutput(PFN pfn, boolean checksumValid) throws Exception {

        GUID guid = pfn.getGuid();

        if(guid == null) {
            throw new Exception("PFN " + pfn.pfn + " has a null GUID");
        }

        if (checksumValid) {
            logger.log(Level.INFO, "Checksum match for PFN " + pfn.pfn);
            mapGuidToPFN.put(guid.guid.toString(), new PFNCrawled(se.seName, pfn.pfn, "xrdstat", "Checkusm match"));
        }
        else {
            logger.log(Level.INFO, "Checksum mismatch PFN " + pfn.pfn);
            mapGuidToPFN.put(guid.guid.toString(), new PFNCrawled(se.seName, pfn.pfn, "xrdstat", "Checkusm mismatch"));
        }
    }

    public static void writeJobOutputToFile(String outputFileType) throws IOException {

        deleteIfOutputExists();

        if(outputFileType.toLowerCase().equals(OUTPUT_FORMAT_JSON)) {
            writeResultAsJSON(mapGuidToPFN);
        }
        else {
            writeResultAsCSV(mapGuidToPFN);
        }
    }

    static void deleteIfOutputExists() throws IOException {
        final String targetFileName = getOutputLFN();
        final LFN l = commander.c_api.getLFN(targetFileName, true);

        if (l.exists) {
            if (!l.delete(true, false)) {
                throw new IOException("Could not delete previous file: " + targetFileName);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void writeResultAsJSON(Map<String, PFNCrawled> mapGuidToPFN) {
        final File outputFile = new File(OUTPUT_FILE_NAME);

        try (FileWriter fw = new FileWriter(outputFile)) {
            JSONObject result = new JSONObject();
            result.put(se.seName, new JSONObject());
            JSONObject files = (JSONObject) result.get(se.seName);
            files.putAll(mapGuidToPFN);
            fw.write(result.toJSONString());
            fw.flush();
            IOUtils.upload(outputFile, getOutputLFN(), commander.getUser(), OUTPUT_LFN_REPLICA_COUNT, null, true);
        }
        catch (IOException exception) {
            logger.log(Level.INFO, "ERROR " + exception.getMessage());
        }
    }

    public static void writeResultAsCSV(Map<String, PFNCrawled> mapGuidToPFN) {
        final File outputFile = new File(OUTPUT_FILE_NAME);

        try (FileWriter fw = new FileWriter(outputFile)) {
            for (Map.Entry<String, PFNCrawled> entry : mapGuidToPFN.entrySet()) {
                String guid = entry.getKey();
                PFNCrawled pfnCrawled = entry.getValue();
                String resultCSV = guid + "," + pfnCrawled.toCSV() + "\n";
                fw.write(resultCSV);
            }
            fw.flush();
            IOUtils.upload(outputFile, getOutputLFN(), commander.getUser(), OUTPUT_LFN_REPLICA_COUNT, null, true);
        }
        catch (IOException exception) {
            logger.log(Level.INFO, "ERROR " + exception.getMessage());
        }
    }

    public static String getOutputLFN() {
        String fileType = outputFileType.equals(OUTPUT_FORMAT_JSON) ? OUTPUT_FORMAT_JSON : OUTPUT_FORMAT_CSV;
        return commander.getCurrentDirName() + "/crawl_output/" + outputDirectoryName + "/" + se.getName().replace("::", "_") + "/" + OUTPUT_FILE_NAME + "." + fileType;
    }
}