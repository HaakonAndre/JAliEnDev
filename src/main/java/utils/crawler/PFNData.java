package utils.crawler;

import org.json.simple.JSONObject;

/**
 * Models the data that is gathered during the crawling process for every PFN
 *
 * @author anegru
 */
class PFNData {

	/**
	 * The name of the storage element the PFN resides in
	 */
	private final String seName;

	/**
	 * The PFN analysed
	 */
	private final String pfn;

	/**
	 * The result of the analysis
	 */
	private final CrawlingStatus crawlingResults;

	/**
	 * Time in milliseconds it takes to download the PFN
	 */
	private final Long downloadDurationMillis;

	/**
	 * Time in milliseconds it takes to call xrdfs
	 */
	private final Long xrdfsDurationMillis;

	/**
	 * The size of the file after download
	 */
	private final Long observedSize;

	/**
	 * The size of the file from the catalogue
	 */
	private final Long catalogueSize;

	/**
	 * The MD5 of the file recomputed after download
	 */
	private final String observedMD5;

	/**
	 * The MD5 of the gile from the catalogue
	 */
	private final String catalogueMD5;


	static final String CSV_HEADER = "Guid,SeName,PFN,ObservedSize,CatalogueSize,ObservedMD5,CatalogueMD5,DownloadDurationMillis,XrdfsDurationMillis,StatusCode,StatusMessage\n";

	PFNData(String seName, String pfn, CrawlingStatus crawlingResults, Long observedSize, Long catalogueSize, String observedMD5, String catalogueMD5, Long downloadDurationMillis, Long xrdfsDurationMillis) {
		this.seName = seName;
		this.pfn = pfn;
		this.crawlingResults = crawlingResults;
		this.observedSize = observedSize;
		this.catalogueSize = catalogueSize;
		this.observedMD5 = observedMD5;
		this.catalogueMD5 = catalogueMD5;
		this.downloadDurationMillis = downloadDurationMillis;
		this.xrdfsDurationMillis = xrdfsDurationMillis;
	}

	public String getSeName() {
		return seName;
	}

	public String getPfn() {
		return pfn;
	}

	String toCSV() {
		return seName + "," + pfn + "," + observedSize + "," + catalogueSize + ","
				+ observedMD5 + "," + catalogueMD5 + "," + downloadDurationMillis + ","
				+ xrdfsDurationMillis + "," + crawlingResults.getCode() + "," + crawlingResults.getMessage();
	}

	@Override
	@SuppressWarnings("unchecked")
	public String toString() {
		JSONObject json = new JSONObject();

		if (seName != null)
			json.put("seName", seName);

		if (pfn != null)
			json.put("pfn", pfn);

		if (observedSize != null)
			json.put("observedSize", observedSize);

		if (catalogueSize != null)
			json.put("catalogueSize", catalogueSize);

		if (observedMD5 != null)
			json.put("observedMD5", observedMD5);

		if (catalogueMD5 != null)
			json.put("catalogueMD5", catalogueMD5);

		if (downloadDurationMillis != null)
			json.put("downloadDurationMillis", downloadDurationMillis);

		if (xrdfsDurationMillis != null)
			json.put("xrdfsDurationMillis", xrdfsDurationMillis);

		if (crawlingResults != null) {
			json.put("statusCode", Integer.valueOf(crawlingResults.getCode()));
			json.put("statusMessage", crawlingResults.getMessage());
		}

		return json.toJSONString();
	}
}
