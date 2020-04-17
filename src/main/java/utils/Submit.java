package utils;

import alien.api.taskQueue.SubmitJob;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.ErrNo;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JAliEnCommandsubmit;
import alien.taskQueue.JDL;
import alien.taskQueue.TaskQueueUtils;
import alien.user.JAKeyStore;
import lazyj.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Submit {

    static final Logger logger = ConfigUtils.getLogger(Crawler.class.getCanonicalName());

    public static void main(String[] args) {
        try {
            logger.log(Level.INFO, "Submitter");

            ConfigUtils.setApplicationName("SubmitJobs");

            if (!JAKeyStore.loadKeyStore()) {
                logger.log(Level.SEVERE, "No identity found, exiting");
                return;
            }

            JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
            System.out.println("Here");

            final String content = "Jobtag = {\"File Crawler\"};\n" +
                    "\n" +
                    "OutputDir = \"/alice/cern.ch/user/a/anegru/crawler/output/$1/\";\n" +
                    "\n" +
                    "\n" +
                    "InputFile = {\n" +
                    "    \"LF:/alice/cern.ch/user/a/anegru/crawler/alien-users.jar\",\n" +
                    "};\n" +
                    "\n" +
                    "Arguments = \"$2 $3\";\n" +
                    "\n" +
                    "Executable = \"executable.sh\";\n" +
                    "\n" +
                    "TTL = \"300\";\n" +
                    "\n" +
                    "Price = \"1\";\n" +
                    "\n" +
                    "FilesToCheck = \"crawler.log\";\n" +
                    "\n" +
                    "Output = {\"crawler.log\"};\n" +
                    "\n" +
                    "JDLVariables = {\"FilesToCheck\"};\n" +
                    "\n" +
                    "Workdirectorysize={\"11000MB\"};";

            logger.log(Level.INFO, "Apply args");

            List<String> sesToQuery = new ArrayList<>();
            List<SE> results = commander.c_api.getSEs(sesToQuery);
            int i = 0;
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

//                jdl.set("Requirements", new StringBuilder("other.CE == \"ALICE::RAL::LCG\""); //(member(other.CloseSE,\"ALICE::RAL::CEPH\"))"));
//                jdl.set("JDLPath", "/alice/cern.ch/user/a/anegru/crawler/crawl.jdl");

                if(i++ == 0) {
                    logger.log(Level.INFO,  jdl.toString());
                }

                logger.log(Level.INFO, "Before submit");
                commander.q_api.submitJob(jdl);
                logger.log(Level.INFO, "After submit");
                break;
            }

        } catch(Exception e) {
            System.out.println(e);
        }
    }
}
