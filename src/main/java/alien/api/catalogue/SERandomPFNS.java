package alien.api.catalogue;

import alien.api.Request;
import alien.catalogue.CatalogueUtils;
import alien.catalogue.PFN;
import alien.se.SEUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class SERandomPFNS extends Request {

    private final Integer seNumber;
    private final Integer fileCount;

    private Collection<PFN> randomPFNs = null;

    public SERandomPFNS(int seNumber, int fileCount) {
        this.seNumber = seNumber;
        this.fileCount = fileCount;
    }

    @Override
    public List<String> getArguments() {
        return Arrays.asList(this.seNumber.toString(), this.fileCount.toString());
    }

    @Override
    public void run() {
        try {
            SEUtils.getRandomPFNs(this.seNumber, this.fileCount);
        }
        catch (Exception e) {

        }
    }

    public Collection<PFN> getRandomPFNs() {
        return randomPFNs;
    }
}
