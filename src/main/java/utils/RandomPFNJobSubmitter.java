package utils;

import alien.config.ConfigUtils;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;
import alien.taskQueue.JDL;
import alien.user.JAKeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author anegru
 * @since Apr 17, 2020
 */
public class RandomPFNJobSubmitter {

    /**
     * logger
     */
    static final Logger logger = ConfigUtils.getLogger(RandomPFNCrawler.class.getCanonicalName());

    /**
     * Entry point where the Submit job starts
     * Submit jobs that extract random PFNs from all SEs and check their hashes.
     * @param args
     */
    public static void main(String[] args) {
        try {
            ConfigUtils.setApplicationName("SubmitJobs");

            if (!JAKeyStore.loadKeyStore()) {
                logger.log(Level.SEVERE, "No identity found, exiting");
                return;
            }

            JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

            List<String> sesToQuery = new ArrayList<>();
            List<SE> results = commander.c_api.getSEs(sesToQuery);

            for(SE se : results) {

                logger.log(Level.INFO, "SE " + se.seName);

                JDL jdl = new JDL();
                jdl.append("JobTag", "FileCrawler");
                jdl.set("OutputDir", "/alice/cern.ch/user/a/anegru/crawler/output/submit/" + se.seName + "/");
                jdl.append("InputFile", "LF:/alice/cern.ch/user/a/anegru/crawler/alien-users.jar");
                jdl.set("Arguments", se.seNumber + " " + 10 + " " + 20);
                jdl.set("Executable", "/alice/cern.ch/user/a/anegru/crawler/executable.sh");
                jdl.set("TTL", 3600);
                jdl.set("Price", 1);
                jdl.set("FilesToCheck", "crawler.log");
                jdl.append("JDLVariables", "FilesToCheck");
                jdl.append("Output", "crawler.log");
                jdl.append("Workdirectorysize", "11000MB");
                jdl.set("Requirements", new StringBuilder("other.CE == \"ALICE::RAL::LCG\""));

                commander.q_api.submitJob(jdl);
            }

        } catch(Exception e) {
            System.out.println(e);
        }
    }
}
