package alien.api.catalogue;

import alien.api.Request;
import alien.se.SEUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class SERandomPFNs extends Request {

    private final Integer seNumber;
    private final Integer fileCount;

    private Collection<String> randomPFNs = null;

    public SERandomPFNs(int seNumber, int fileCount) {
        this.seNumber = seNumber;
        this.fileCount = fileCount;
    }

    @Override
    public List<String> getArguments() {
        return Arrays.asList(this.seNumber.toString(), this.fileCount.toString());
    }

    @Override
    public void run() {
        this.randomPFNs = SEUtils.getRandomPFNs(this.seNumber, this.fileCount);
    }

    public Collection<String> getPFNS() {
        return randomPFNs;
    }
}
