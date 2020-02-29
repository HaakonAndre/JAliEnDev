package alien.shell.commands;

import java.util.Collection;
import java.util.List;

public class JAliEnCommandrandomPFNs extends JAliEnBaseCommand {

    private Integer seNumber;
    private Integer fileCount;

    @Override
    public void run() {
        Collection<String> randomPFNs = commander.c_api.getRandomPFNsFromSE(seNumber, fileCount);
        commander.printOutln(randomPFNs.toString());
    }

    @Override
    public void printHelp() {
        commander.printOutln();
        commander.printOutln("Extract <fileCount> random PFNs from the SE with number <seNumber>");
        commander.printOutln(helpUsage("randomPFNs", " <seNumber>  <fileCount>"));
        commander.printOutln();
    }

    @Override
    public boolean canRunWithoutArguments() {
        return false;
    }

    public JAliEnCommandrandomPFNs(JAliEnCOMMander commander, List<String> alArguments) {
        super(commander, alArguments);
        seNumber =  Integer.parseInt(alArguments.get(0));
        fileCount =  Integer.parseInt(alArguments.get(1));
        setArgumentsOk(true);
    }
}