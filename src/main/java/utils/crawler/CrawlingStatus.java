package utils.crawler;

/**
 * The crawling status for each PFN analysed
 *
 * @author anegru
 */
enum CrawlingStatus {

	/**
	 * Success. Checksum from catalogue matches recomputed checksum
	 */

	S_FILE_CHECKSUM_MATCH(1, CrawlingStatusType.FILE_OK, "Checksum from catalogue matches recomputed checksum"),

	/**
	 * Error. Computation of MD5 failed
	 */
	E_FILE_MD5_COMPUTATION_FAILED(2, CrawlingStatusType.FILE_INACCESSIBLE, "Computation of MD5 failed"),

	/**
	 * Error. LFN cannot be found
	 */
	E_LFN_NOT_FOUND(3, CrawlingStatusType.FILE_INACCESSIBLE, "LFN cannot be found"),

	/**
	 * Error. LFN does not exist
	 */
	E_LFN_DOES_NOT_EXIST(4, CrawlingStatusType.FILE_INACCESSIBLE, "LFN does not exist"),

	/**
	 * Error. Checksum from catalogue does not match recomputed checksum
	 */
	E_FILE_CHECKSUM_MISMATCH(5, CrawlingStatusType.FILE_CORRUPT, "Checksum from catalogue does not match recomputed checksum"),

	/**
	 * Error. Observed file size is 0
	 */
	E_FILE_EMPTY(6, CrawlingStatusType.FILE_CORRUPT, "Observed file size is 0"),

	/**
	 * Error. Observed file size does not match size from the catalogue
	 */
	E_FILE_SIZE_MISMATCH(7, CrawlingStatusType.FILE_CORRUPT, "Observed file size does not match size from the catalogue"),

	/**
	 * Error. MD5 value from the catalogue is null
	 */
	E_FILE_MD5_IS_NULL(8, CrawlingStatusType.FILE_CORRUPT, "MD5 value from the catalogue is null"),

	/**
	 * Error. Cannot get PFN access for reading
	 */
	E_PFN_NOT_READABLE(9, CrawlingStatusType.FILE_INACCESSIBLE, "Cannot get PFN access for reading"),

	/**
	 * Error. PFN is not online
	 */
	E_PFN_NOT_ONLINE(10, CrawlingStatusType.FILE_INACCESSIBLE, "PFN is not online"),

	/**
	 * Error. Cannot get GUID from PFN
	 */
	E_GUID_NOT_FOUND(11, CrawlingStatusType.FILE_INACCESSIBLE, "Cannot get GUID from PFN"),

	/**
	 * Error. PFN cannot be downloaded
	 */
	E_PFN_DOWNLOAD_FAILED(12, CrawlingStatusType.FILE_INACCESSIBLE, "PFN cannot be downloaded"),

	/**
	 * Error. PFN cannot be downloaded
	 */
	E_XROOTD_RETURNS_NULL(13, CrawlingStatusType.FILE_INACCESSIBLE, "PFN cannot be downloaded"),

	/**
	 * Error. The error encountered is unexpected
	 */
	E_UNEXPECTED_ERROR(14, CrawlingStatusType.UNEXPECTED_ERROR, "The error encountered is unexpected");


	/**
	 * Status code
	 */
	private final int code;

	/**
	 * CrawlingStatusType of the status.
	 */
	private final CrawlingStatusType type;

	/**
	 * Default error message
	 */
	private final String message;

	CrawlingStatus(final int code, final CrawlingStatusType type, final String message) {
		this.code = code;
		this.type = type;
		this.message = message;
	}

	/**
	 * @return the status code associated to this constant
	 */
	public int getCode() {
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
