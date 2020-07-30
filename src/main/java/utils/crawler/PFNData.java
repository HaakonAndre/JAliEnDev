package utils.crawler;

import org.json.simple.JSONObject;

/**
 * @author anegru
 */
class PFNData {

	private final String seName;
	private final String pfn;
	private final CrawlingResult crawlingResults;
	private final Long downloadDurationMillis;
	private final Long xrdfsDurationMillis;
	private final Long observedSize;
	private final Long catalogueSize;
	private final String observedMD5;
	private final String catalogueMD5;

	public PFNData(String seName, String pfn, CrawlingResult crawlingResults, Long observedSize, Long catalogueSize,
	               String observedMD5, String catalogueMD5, Long downloadDurationMillis, Long xrdfsDurationMillis) {
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

	public String toCSV() {
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
			json.put("statusCode", crawlingResults.getCode());
			json.put("statusMessage", crawlingResults.getMessage());
		}

		return json.toJSONString();
	}

	public static String csvHeader() {
		return "SeName,PFN,StatusCode,StatusMessage,ObservedSize,CatalogueSize,ObservedMD5,CatalogueMD5,DownloadDurationMillis,XrdfsDurationMillis,seName,StatusCode,StatusMessage\n";
	}
}
