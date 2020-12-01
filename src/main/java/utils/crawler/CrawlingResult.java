package utils.crawler;

class CrawlingResult {

	/**
	 * The status of the crawling
	 */
	private CrawlingStatus status;

	/**
	 * Total file size analysed
	 */
	private long fileSizeTotalBytes;

	/**
	 * Total duration of file download in milliseconds
	 */
	private long downloadTotalDurationMillis;

	/**
	 * Total number of PFNs that could be downloaded
	 */
	private long downloadedPFNsTotalCount;

	/**
	 * Total duration of xrdfs in milliseconds
	 */
	private long xrdfsTotalDurationMillis;

	/**
	 * Total number of PFNs that were tested with xrdfs
	 */
	private long xrdfsPFNsTotalCount;

	CrawlingResult(CrawlingStatus status, long fileSizeTotalBytes, long downloadTotalDurationMillis, long downloadedPFNsTotalCount, long xrdfsTotalDurationMillis, long xrdfsPFNsTotalCount) {
		this.status = status;
		this.fileSizeTotalBytes = fileSizeTotalBytes;
		this.downloadTotalDurationMillis = downloadTotalDurationMillis;
		this.downloadedPFNsTotalCount = downloadedPFNsTotalCount;
		this.xrdfsTotalDurationMillis = xrdfsTotalDurationMillis;
		this.xrdfsPFNsTotalCount = xrdfsPFNsTotalCount;
	}

	public CrawlingStatus getStatus() {
		return status;
	}

	long getFileSizeTotalBytes() {
		return fileSizeTotalBytes;
	}

	long getDownloadTotalDurationMillis() {
		return downloadTotalDurationMillis;
	}

	long getDownloadedPFNsTotalCount() {
		return downloadedPFNsTotalCount;
	}

	long getXrdfsTotalDurationMillis() {
		return xrdfsTotalDurationMillis;
	}

	long getXrdfsPFNsTotalCount() {
		return xrdfsPFNsTotalCount;
	}

	@Override
	public String toString() {
		return "CrawlingResult{" + "status=" + status + ", fileSizeTotalBytes=" + fileSizeTotalBytes + ", downloadTotalDurationMillis=" + downloadTotalDurationMillis + ", downloadedPFNsTotalCount=" + downloadedPFNsTotalCount + ", xrdfsTotalDurationMillis=" + xrdfsTotalDurationMillis + ", xrdfsPFNsTotalCount=" + xrdfsPFNsTotalCount + '}';
	}
}
