package utils.crawler;

public class PFNCrawled {
    private String seName;
    private String pfn;
    private String checkMethod;
    private String message;

    public PFNCrawled(String seName, String pfn, String checkMethod, String message) {
        this.seName = seName;
        this.pfn = pfn;
        this.checkMethod = checkMethod;
        this.message = message;
    }

    public String getSeName() {
        return seName;
    }

    public String getPfn() {
        return pfn;
    }

    public String getCheckMethod() {
        return checkMethod;
    }

    public String getMessage() {
        return message;
    }

    public String toCSV() {
        return seName + "," + pfn + "," + checkMethod + "," + message;
    }

    @Override
    public String toString() {
        return "{" +
                "\"seName\":\"" + seName + "\""+
                ",\"pfn\": \"" + pfn + "\"" +
                ", \"checkMethod\": \"" + checkMethod + "\"" +
                ", \"message\": \"" + message + "\"" + '}';
    }
}
