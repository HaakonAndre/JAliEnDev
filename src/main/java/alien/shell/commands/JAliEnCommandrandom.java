package alien.shell.commands;

import alien.catalogue.PFN;

import java.util.Collection;
import java.util.List;

public class JAliEnCommandrandom extends JAliEnBaseCommand {

    private Integer seNumber;
    private Integer fileCount;

    @Override
    public void run() {
        Collection<PFN> randomPFNs = commander.c_api.getRandomFilesFromSE(seNumber, fileCount);
        commander.printOutln(randomPFNs.toString());
    }

    @Override
    public void printHelp() {
        commander.printOutln();
        commander.printOutln("Extract <fileCount> random files from the SE with number <seNumber>");
        commander.printOutln(helpUsage("random", " <seNumber>  <fileCount>"));
        commander.printOutln();
    }

    @Override
    public boolean canRunWithoutArguments() {
        return false;
    }

    public JAliEnCommandrandom(JAliEnCOMMander commander, List<String> alArguments) {
        super(commander, alArguments);
        seNumber =  Integer.parseInt(alArguments.get(0));
        fileCount =  Integer.parseInt(alArguments.get(1));
        setArgumentsOk(true);
    }
}