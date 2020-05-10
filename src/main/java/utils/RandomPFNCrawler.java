package utils;

import alien.catalogue.GUID;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.protocols.Factory;
import alien.io.protocols.Xrootd;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.user.JAKeyStore;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author anegru
 * @since Apr 17, 2020
 */
public class RandomPFNCrawler {

    /**
     * logger
     */
    static final Logger logger = ConfigUtils.getLogger(RandomPFNCrawler.class.getCanonicalName());
    public static AtomicInteger downloadCount = new AtomicInteger(0);

    /**
     * 3 command line arguments must be provided:
     *
     * integer seNumber
     * integer fileCount
     * integer jobDurationSeconds
     *
     * Extract random PFNs from storage element with number seNumber. The job can last for at most
     * jobDurationSeconds seconds. Initially extract fileCount elements. If the duration is not exceeded
     * and there is enough time for more files to be extracted, the fileCount is doubled at each iteration.
     *
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

        if (args.length != 3) {
            logger.log(Level.SEVERE, "Crawler must be run with 3 integer arguments: seNumber, fileCount, jobDurationSeconds");
            return;
        }

        JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

        List<String> ses = new ArrayList<>();
        List<String> exses = new ArrayList<>();

        int seNumber = Integer.parseInt(args[0]);
        int fileCount = Integer.parseInt(args[1]);
        int jobDurationSeconds = Integer.parseInt(args[2]);

        SE storageElement = SEUtils.getSE(seNumber);

        logger.log(Level.INFO, "Arguments " + seNumber + " " + fileCount + " " + jobDurationSeconds);

        if (storageElement == null) {
            logger.log(Level.INFO, "Storage element with number " + seNumber + " does not exist");
            return;
        }

        if (jobDurationSeconds < 0) {
            logger.log(Level.INFO, "Job duration must be a positive integer");
            return;
        }

        long globalStartTimestamp = System.currentTimeMillis();
        long globalEndTimestamp = globalStartTimestamp + jobDurationSeconds * 1000;
        long currentTimestamp = globalStartTimestamp;

        ses.add(storageElement.seName);
        Xrootd xrootd = (Xrootd) Factory.xrootd.clone();

        long chunkStartTimestamp, chunkEndTimestamp;

        logger.log(Level.INFO, "This job will run for " + (globalEndTimestamp - globalStartTimestamp) / 1000.0);

        while (currentTimestamp < globalEndTimestamp) {
            chunkStartTimestamp = System.currentTimeMillis();

            Collection<PFN> randomPFNs = commander.c_api.getRandomPFNsFromSE(seNumber, fileCount);
            ExecutorService executorService = new CachedThreadPool(16, jobDurationSeconds, TimeUnit.SECONDS);

            for (PFN currentPFN : randomPFNs) {
                executorService.submit(new DownloadPFN(currentPFN, xrootd, commander, storageElement, logger));
            }

            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(jobDurationSeconds, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }

            chunkEndTimestamp = System.currentTimeMillis();
            long chunkDuration = chunkEndTimestamp - chunkStartTimestamp;
            long durationPerFile = chunkDuration / fileCount;
            currentTimestamp = System.currentTimeMillis();

            logger.log(Level.INFO, "Iteration took This job will run for " + chunkDuration / 1000.0 + " " + chunkDuration);
            logger.log(Level.INFO, "Iteration single file took " + durationPerFile / 1000.0 + " " + durationPerFile);
            logger.log(Level.INFO, "Comparision is " + (currentTimestamp + durationPerFile * (fileCount * 2)) + " < " + globalEndTimestamp);

            if(currentTimestamp + durationPerFile * (fileCount * 2) < globalEndTimestamp) {
                fileCount *= 2;
            }
        }

        logger.log(Level.INFO, "The number of files analysed " + downloadCount);
    }
}

class DownloadPFN implements Runnable {

    private PFN pfnToDownload;
    private Xrootd xrootd;
    private JAliEnCOMMander commander;
    private SE storageElement;
    private Logger logger;

    public DownloadPFN(PFN pfnToDownload, Xrootd xrootd, JAliEnCOMMander commander, SE storageElement, Logger logger) {
        this.pfnToDownload = pfnToDownload;
        this.xrootd = xrootd;
        this.commander = commander;
        this.storageElement = storageElement;
        this.logger = logger;
    }

    @Override
    public void run() {

        List<String> ses = new ArrayList<>();
        List<String> exses = new ArrayList<>();

        ses.add(storageElement.seName);

        try {
            GUID guid = pfnToDownload.getGuid();

            if (guid != null) {

                String md5RecomputedAfterDownload, md5FromCatalogue = guid.md5;
                Collection<PFN> pfnsToRead = commander.c_api.getPFNsToRead(guid, ses, exses);
                Iterator<PFN> pfnIterator = pfnsToRead.iterator();

                if (pfnIterator.hasNext()) {

                    PFN pfnToRead = pfnIterator.next();
                    long downloadStartTimestamp = System.currentTimeMillis();
                    File downloadedFile = xrootd.get(pfnToRead, null);
                    long downloadEndTimestamp = System.currentTimeMillis();
                    long downloadDuration = downloadEndTimestamp - downloadStartTimestamp;

                    if (downloadedFile != null) {
                        md5RecomputedAfterDownload = IOUtils.getMD5(downloadedFile);
                        if (md5FromCatalogue.equals(md5RecomputedAfterDownload)) {
                            logger.log(Level.INFO, "Checksum match for PFN " + pfnToDownload.pfn + " Download took " + downloadDuration + " ms");
                        } else {
                            logger.log(Level.INFO, "Checksum mismatch PFN " + pfnToDownload.pfn + " Download took " + downloadDuration + " ms");
                        }
                        boolean fileDeleted = downloadedFile.delete();

                        RandomPFNCrawler.downloadCount.incrementAndGet();

                        if(!fileDeleted) {
                            logger.log(Level.INFO, "Downloaded file cannot be deleted");
                        }
                    } else {
                        logger.log(Level.INFO, "Cannot download  " + pfnToDownload.pfn);
                    }
                } else {
                    logger.log(Level.INFO, "No PFNs to read for PFN " + pfnToDownload.pfn);
                }
            } else {
                logger.log(Level.INFO, "GUID is null for PFN " + pfnToDownload.pfn);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.log(Level.INFO, "ERROR at PFN " + pfnToDownload.pfn + " " + e.getMessage());
        }
    }
}