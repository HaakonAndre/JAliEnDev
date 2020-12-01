package utils.crawler;

/**
 * The crawling status for each PFN analysed
 *
 * @author anegru
 */
public class CrawlingStatus {

	/**
	 * Crawling status code
	 */
	private Enum code;


	/**
	 * CrawlingStatusType of the status.
	 */
	private final CrawlingStatusType type;

	/**
	 * Default error message
	 */
	private final String message;

	public CrawlingStatus(Enum code, final CrawlingStatusType type, final String message) {
		this.code = code;
		this.type = type;
		this.message = message;
	}

	/**
	 * @return the status code associated to this constant
	 */
	public Enum getCode() {
		return code;
	}

	/**
	 * @return the status type associated to this constant
	 */
	public CrawlingStatusType getCrawlingStatusType() {
		return type;
	}

	/**
	 * @return message for this constant
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * @param crawlingStatusType The type that must be checked
	 * @return true if the crawling result matches the parameter, false otherwise
	 */
	public boolean statusHasType(CrawlingStatusType crawlingStatusType) {
		return this.type == crawlingStatusType;
	}

	@Override
	public String toString() {
		return "CrawlingStatus{" + "code=" + code + ", type=" + type + ", message='" + message + '\'' + '}';
	}
}