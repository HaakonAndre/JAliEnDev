package utils.crawler;

import org.json.simple.JSONObject;

import java.util.List;

/**
 * Class that models the crawling statistics that are gathered in each iteration.
 * All fields are nullable, thus optional. If one field is null it means that it cannot be retrieved
 * from disk or other errors occurred. Null values are skipped.
 *
 * @author anegru
 */
class CrawlingStatistics {

	/**
	 * The total number of PFNs analysed.
	 */
	long pfnCount;

	/**
	 * The number of PFNs that are ok from the total number of PFNs crawled
	 */
	long pfnOkCount;

	/**
	 * The number of PFNs that are inaccessible from the total number of PFNs crawled
	 */
	long pfnInaccessibleCount;

	/**
	 * The number of PFNs that are corrupt from the total number of PFNs crawled
	 */
	long pfnCorruptCount;

	/**
	 * The number of PFNs whose status is unknown from the total number of PFNs crawled
	 */
	long pfnUnknownStatusCount;

	/**
	 * Minimum duration in milliseconds of the crawling process for one single PFN
	 */
	long crawlingMinDurationMillis;

	/**
	 * Maximum duration in milliseconds of the crawling process for one single PFN
	 */
	long crawlingMaxDurationMillis;

	/**
	 * Average duration in milliseconds of the crawling process
	 */
	long crawlingAvgDurationMillis;

	/**
	 * Total duration in milliseconds of the crawling process
	 */
	long crawlingTotalDurationMillis;

	/**
	 * Total duration in milliseconds of the current iteration
	 */
	long iterationTotalDurationMillis;

	/**
	 * Total size of files in bytes downloaded during the crawling process
	 */
	long fileSizeTotalBytes;

	/**
	 * Total duration of file download in milliseconds
	 */
	long downloadTotalDurationMillis;

	/**
	 * Total number of PFNs that could be downloaded
	 */
	long downloadedPFNsTotalCount;

	/**
	 * Total duration of xrdfs in milliseconds
	 */
	long xrdfsTotalDurationMillis;

	/**
 		* Total number of PFNs that were tested with xrdfs
 	*/
	long xrdfsPFNsTotalCount;

	private static final int NULL_VALUE = -1;
	private static final int DEFAULT_VALUE = 0;

	private CrawlingStatistics() {
		this.pfnCount = NULL_VALUE;
		this.pfnOkCount = NULL_VALUE;
		this.pfnInaccessibleCount = NULL_VALUE;
		this.pfnCorruptCount = NULL_VALUE;
		this.pfnUnknownStatusCount = NULL_VALUE;
		this.crawlingMinDurationMillis = Long.MAX_VALUE;
		this.crawlingMaxDurationMillis = Long.MAX_VALUE;
		this.crawlingAvgDurationMillis = NULL_VALUE;
		this.crawlingTotalDurationMillis = NULL_VALUE;
		this.iterationTotalDurationMillis = NULL_VALUE;
		this.fileSizeTotalBytes = NULL_VALUE;
		this.downloadTotalDurationMillis = NULL_VALUE;
		this.downloadedPFNsTotalCount = NULL_VALUE;
		this.xrdfsTotalDurationMillis = NULL_VALUE;
		this.xrdfsPFNsTotalCount = NULL_VALUE;
	}

	CrawlingStatistics(long pfnCount, long pfnOkCount, long pfnInaccessibleCount, long pfnCorruptCount, long pfnUnknownStatusCount,
			long crawlingMinDurationMillis,  long crawlingMaxDurationMillis, long crawlingAvgDurationMillis, long crawlingTotalDurationMillis,
			long iterationTotalDurationMillis, long fileSizeTotalBytes, long downloadedPFNsTotalCount, long downloadTotalDurationMillis,
			long xrdfsPFNsTotalCount, long xrdfsTotalDurationMillis) {
		this.pfnCount = pfnCount;
		this.pfnOkCount = pfnOkCount;
		this.pfnInaccessibleCount = pfnInaccessibleCount;
		this.pfnCorruptCount = pfnCorruptCount;
		this.pfnUnknownStatusCount = pfnUnknownStatusCount;
		this.crawlingMinDurationMillis = crawlingMinDurationMillis;
		this.crawlingMaxDurationMillis = crawlingMaxDurationMillis;
		this.crawlingAvgDurationMillis = crawlingAvgDurationMillis;
		this.crawlingTotalDurationMillis = crawlingTotalDurationMillis;
		this.iterationTotalDurationMillis = iterationTotalDurationMillis;
		this.fileSizeTotalBytes = fileSizeTotalBytes;
		this.downloadedPFNsTotalCount = downloadedPFNsTotalCount;
		this.downloadTotalDurationMillis = downloadTotalDurationMillis;
		this.xrdfsPFNsTotalCount = xrdfsPFNsTotalCount;
		this.xrdfsTotalDurationMillis = xrdfsTotalDurationMillis;
	}

	static CrawlingStatistics fromJSON(JSONObject jsonObject) {
		return new CrawlingStatistics(
				getLongFromJSON(jsonObject, "pfnCount"),
				getLongFromJSON(jsonObject, "pfnOkCount"),
				getLongFromJSON(jsonObject, "pfnInaccessibleCount"),
				getLongFromJSON(jsonObject, "pfnCorruptCount"),
				getLongFromJSON(jsonObject, "pfnUnknownStatusCount"),
				getLongFromJSON(jsonObject, "crawlingMinDurationMillis"),
				getLongFromJSON(jsonObject, "crawlingMaxDurationMillis"),
				getLongFromJSON(jsonObject, "crawlingAvgDurationMillis"),
				getLongFromJSON(jsonObject, "crawlingTotalDurationMillis"),
				getLongFromJSON(jsonObject, "iterationTotalDurationMillis"),
				getLongFromJSON(jsonObject, "fileSizeTotalBytes"),
				getLongFromJSON(jsonObject, "downloadedPFNsTotalCount"),
				getLongFromJSON(jsonObject, "downloadTotalDurationMillis"),
				getLongFromJSON(jsonObject, "xrdfsPFNsTotalCount"),
				getLongFromJSON(jsonObject, "xrdfsTotalDurationMillis")
		);
	}

	@SuppressWarnings("unchecked")
	static JSONObject toJSON(CrawlingStatistics stats) {

		JSONObject json = new JSONObject();

		json.put("pfnCount", getValueForJSON(stats.pfnCount));
		json.put("pfnOkCount", getValueForJSON(stats.pfnOkCount));
		json.put("pfnInaccessibleCount", getValueForJSON(stats.pfnInaccessibleCount));
		json.put("pfnCorruptCount", getValueForJSON(stats.pfnCorruptCount));
		json.put("pfnUnknownStatusCount", getValueForJSON(stats.pfnUnknownStatusCount));
		json.put("crawlingMinDurationMillis", getValueForJSON(stats.crawlingMinDurationMillis));
		json.put("crawlingMaxDurationMillis", getValueForJSON(stats.crawlingMaxDurationMillis));
		json.put("crawlingAvgDurationMillis", getValueForJSON(stats.crawlingAvgDurationMillis));
		json.put("crawlingTotalDurationMillis", getValueForJSON(stats.crawlingTotalDurationMillis));
		json.put("iterationTotalDurationMillis", getValueForJSON(stats.iterationTotalDurationMillis));
		json.put("fileSizeTotalBytes", getValueForJSON(stats.fileSizeTotalBytes));
		json.put("downloadedPFNsTotalCount", getValueForJSON(stats.downloadedPFNsTotalCount));
		json.put("downloadTotalDurationMillis", getValueForJSON(stats.downloadTotalDurationMillis));
		json.put("xrdfsPFNsTotalCount", getValueForJSON(stats.xrdfsPFNsTotalCount));
		json.put("xrdfsTotalDurationMillis", getValueForJSON(stats.xrdfsTotalDurationMillis));

		return json;
	}

	private static Long getValueForJSON(long value) {
		return  value != NULL_VALUE ? Long.valueOf(value) : null;
	}

	static CrawlingStatistics getAveragedStats(List<CrawlingStatistics> statsList) {

		if (statsList == null || statsList.size() == 0)
			return null;

		CrawlingStatistics averagedStats = new CrawlingStatistics();
		int crawlingAvgDurationCount = 0;

		for (CrawlingStatistics stats : statsList) {

			if (stats.crawlingMinDurationMillis != NULL_VALUE && stats.crawlingMinDurationMillis < averagedStats.crawlingMinDurationMillis)
				averagedStats.crawlingMinDurationMillis = stats.crawlingMinDurationMillis;

			if (stats.crawlingMaxDurationMillis != NULL_VALUE && stats.crawlingMaxDurationMillis > averagedStats.crawlingMaxDurationMillis)
				averagedStats.crawlingMaxDurationMillis = stats.crawlingMaxDurationMillis;

			if (stats.iterationTotalDurationMillis != NULL_VALUE && stats.iterationTotalDurationMillis > averagedStats.iterationTotalDurationMillis)
				averagedStats.iterationTotalDurationMillis = stats.iterationTotalDurationMillis;

			if (stats.pfnOkCount != NULL_VALUE) {
				averagedStats.pfnOkCount = initializeIfNull(stats.pfnOkCount);
				averagedStats.pfnOkCount += stats.pfnOkCount;
			}

			if (stats.pfnInaccessibleCount != NULL_VALUE) {
				averagedStats.pfnInaccessibleCount = initializeIfNull(stats.pfnInaccessibleCount);
				averagedStats.pfnInaccessibleCount += stats.pfnInaccessibleCount;
			}

			if (stats.pfnCorruptCount != NULL_VALUE) {
				averagedStats.pfnCorruptCount = initializeIfNull(stats.pfnCorruptCount);
				averagedStats.pfnCorruptCount += stats.pfnCorruptCount;
			}

			if (stats.pfnUnknownStatusCount != NULL_VALUE) {
				averagedStats.pfnUnknownStatusCount = initializeIfNull(stats.pfnUnknownStatusCount);
				averagedStats.pfnUnknownStatusCount += stats.pfnUnknownStatusCount;
			}

			if (stats.fileSizeTotalBytes != NULL_VALUE) {
				averagedStats.fileSizeTotalBytes = initializeIfNull(stats.fileSizeTotalBytes);
				averagedStats.fileSizeTotalBytes += stats.fileSizeTotalBytes;
			}

			if (stats.crawlingAvgDurationMillis != NULL_VALUE) {
				averagedStats.crawlingAvgDurationMillis = initializeIfNull(stats.crawlingAvgDurationMillis);
				averagedStats.crawlingAvgDurationMillis += stats.crawlingAvgDurationMillis;
				crawlingAvgDurationCount += 1;
			}

			if (stats.pfnCount != NULL_VALUE) {
				averagedStats.pfnCount = initializeIfNull(stats.pfnCount);
				averagedStats.pfnCount += stats.pfnCount;
			}

			if (stats.crawlingTotalDurationMillis != NULL_VALUE) {
				averagedStats.crawlingTotalDurationMillis = initializeIfNull(stats.crawlingTotalDurationMillis);
				averagedStats.crawlingTotalDurationMillis += stats.crawlingTotalDurationMillis;
			}

			if (stats.downloadedPFNsTotalCount != NULL_VALUE) {
				averagedStats.downloadedPFNsTotalCount = initializeIfNull(stats.downloadedPFNsTotalCount);
				averagedStats.downloadedPFNsTotalCount += stats.downloadedPFNsTotalCount;
			}

			if (stats.downloadTotalDurationMillis != NULL_VALUE) {
				averagedStats.downloadTotalDurationMillis = initializeIfNull(stats.downloadTotalDurationMillis);
				averagedStats.downloadTotalDurationMillis += stats.downloadTotalDurationMillis;
			}

			if (stats.xrdfsPFNsTotalCount != NULL_VALUE) {
				averagedStats.xrdfsPFNsTotalCount = initializeIfNull(stats.xrdfsPFNsTotalCount);
				averagedStats.xrdfsPFNsTotalCount += stats.xrdfsPFNsTotalCount;
			}

			if (stats.xrdfsTotalDurationMillis != NULL_VALUE) {
				averagedStats.xrdfsTotalDurationMillis = initializeIfNull(stats.xrdfsTotalDurationMillis);
				averagedStats.xrdfsTotalDurationMillis += stats.xrdfsTotalDurationMillis;
			}
		}

		averagedStats.crawlingAvgDurationMillis = crawlingAvgDurationCount == 0 ? NULL_VALUE : averagedStats.crawlingAvgDurationMillis / crawlingAvgDurationCount;

		return averagedStats;
	}

	private static long initializeIfNull(long value) {
		return value == NULL_VALUE ? DEFAULT_VALUE : value;
	}

	private static long getLongFromJSON(JSONObject json, String key) {
		try {
			return ((Long) json.get(key)).longValue();
		}
		catch (Exception e) {
			e.printStackTrace();
			return NULL_VALUE;
		}
	}

	@Override
	public String toString() {
		return toJSON(this).toJSONString();
	}
}
