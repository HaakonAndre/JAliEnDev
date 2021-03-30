package utils.crawler;

import java.util.List;

import org.json.simple.JSONObject;

/**
 * Class that models the crawling statistics that are gathered in each iteration.
 * All fields are nullable, thus optional. If one field is null it means that it cannot be retrieved
 * from disk or other errors occurred. Null values are skipped.
 *
 * @author anegru
 */
public class CrawlingStatistics {

	/**
	 * The total number of PFNs analysed.
	 */
	public Long pfnCount;

	/**
	 * The number of PFNs that are ok from the total number of PFNs crawled
	 */
	public Long pfnOkCount;

	/**
	 * The number of PFNs that are inaccessible from the total number of PFNs crawled
	 */
	public Long pfnInaccessibleCount;

	/**
	 * The number of PFNs that are corrupt from the total number of PFNs crawled
	 */
	public Long pfnCorruptCount;

	/**
	 * The number of PFNs whose status is unknown from the total number of PFNs crawled
	 */
	public Long pfnUnknownStatusCount;

	/**
	 * Average duration in milliseconds of the crawling process
	 */
	public Long crawlingAvgDurationMillis;

	/**
	 * Total duration in milliseconds of the crawling process
	 */
	public Long crawlingTotalDurationMillis;

	/**
	 * Total size of files in bytes downloaded during the crawling process
	 */
	public Long fileSizeTotalBytes;

	/**
	 * Total duration of file download in milliseconds
	 */
	public Long downloadTotalDurationMillis;

	/**
	 * Total number of PFNs that could be downloaded
	 */
	public Long downloadedPFNsTotalCount;

	/**
	 * Total duration of xrdfs in milliseconds
	 */
	public Long xrdfsTotalDurationMillis;

	/**
	 * Total number of PFNs that were tested with xrdfs
	 */
	public Long xrdfsPFNsTotalCount;

	/**
	 * The Unix timestamp when these statistics are written to disk
	 */
	public Long statGeneratedUnixTimestamp;

	private CrawlingStatistics() {

	}

	/**
	 * @param pfnCount
	 * @param pfnOkCount
	 * @param pfnInaccessibleCount
	 * @param pfnCorruptCount
	 * @param pfnUnknownStatusCount
	 * @param crawlingAvgDurationMillis
	 * @param crawlingTotalDurationMillis
	 * @param fileSizeTotalBytes
	 * @param downloadedPFNsTotalCount
	 * @param downloadTotalDurationMillis
	 * @param xrdfsPFNsTotalCount
	 * @param xrdfsTotalDurationMillis
	 * @param statGeneratedUnixTimestamp
	 */
	public CrawlingStatistics(final Long pfnCount, final Long pfnOkCount, final Long pfnInaccessibleCount, final Long pfnCorruptCount, final Long pfnUnknownStatusCount,
			final Long crawlingAvgDurationMillis, final Long crawlingTotalDurationMillis, final Long fileSizeTotalBytes,
			final Long downloadedPFNsTotalCount, final Long downloadTotalDurationMillis, final Long xrdfsPFNsTotalCount, final Long xrdfsTotalDurationMillis, final Long statGeneratedUnixTimestamp) {
		this.pfnCount = pfnCount;
		this.pfnOkCount = pfnOkCount;
		this.pfnInaccessibleCount = pfnInaccessibleCount;
		this.pfnCorruptCount = pfnCorruptCount;
		this.pfnUnknownStatusCount = pfnUnknownStatusCount;
		this.crawlingAvgDurationMillis = crawlingAvgDurationMillis;
		this.crawlingTotalDurationMillis = crawlingTotalDurationMillis;
		this.fileSizeTotalBytes = fileSizeTotalBytes;
		this.downloadedPFNsTotalCount = downloadedPFNsTotalCount;
		this.downloadTotalDurationMillis = downloadTotalDurationMillis;
		this.xrdfsPFNsTotalCount = xrdfsPFNsTotalCount;
		this.xrdfsTotalDurationMillis = xrdfsTotalDurationMillis;
		this.statGeneratedUnixTimestamp = statGeneratedUnixTimestamp;
	}

	/**
	 * @param jsonObject
	 * @return
	 */
	public static CrawlingStatistics fromJSON(final JSONObject jsonObject) {
		return new CrawlingStatistics(
				getLongFromJSON(jsonObject, "pfnCount"),
				getLongFromJSON(jsonObject, "pfnOkCount"),
				getLongFromJSON(jsonObject, "pfnInaccessibleCount"),
				getLongFromJSON(jsonObject, "pfnCorruptCount"),
				getLongFromJSON(jsonObject, "pfnUnknownStatusCount"),
				getLongFromJSON(jsonObject, "crawlingAvgDurationMillis"),
				getLongFromJSON(jsonObject, "crawlingTotalDurationMillis"),
				getLongFromJSON(jsonObject, "fileSizeTotalBytes"),
				getLongFromJSON(jsonObject, "downloadedPFNsTotalCount"),
				getLongFromJSON(jsonObject, "downloadTotalDurationMillis"),
				getLongFromJSON(jsonObject, "xrdfsPFNsTotalCount"),
				getLongFromJSON(jsonObject, "xrdfsTotalDurationMillis"),
				getLongFromJSON(jsonObject, "statGeneratedUnixTimestamp"));
	}

	/**
	 * Convert CrawlingStatistics object to JSONObject
	 * @param stats
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static JSONObject toJSON(final CrawlingStatistics stats) {

		final JSONObject json = new JSONObject();

		json.put("pfnCount", stats.pfnCount);
		json.put("pfnOkCount", stats.pfnOkCount);
		json.put("pfnInaccessibleCount", stats.pfnInaccessibleCount);
		json.put("pfnCorruptCount", stats.pfnCorruptCount);
		json.put("pfnUnknownStatusCount", stats.pfnUnknownStatusCount);
		json.put("crawlingAvgDurationMillis", stats.crawlingAvgDurationMillis);
		json.put("crawlingTotalDurationMillis", stats.crawlingTotalDurationMillis);
		json.put("fileSizeTotalBytes", stats.fileSizeTotalBytes);
		json.put("downloadedPFNsTotalCount", stats.downloadedPFNsTotalCount);
		json.put("downloadTotalDurationMillis", stats.downloadTotalDurationMillis);
		json.put("xrdfsPFNsTotalCount", stats.xrdfsPFNsTotalCount);
		json.put("xrdfsTotalDurationMillis", stats.xrdfsTotalDurationMillis);
		json.put("statGeneratedUnixTimestamp", stats.statGeneratedUnixTimestamp);

		return json;
	}

	/**
	 * Get the average statistics from a list of CrawlingStatistics objects
	 * @param statsList
	 * @return
	 */
	public static CrawlingStatistics getAveragedStats(final List<CrawlingStatistics> statsList) {

		if (statsList == null || statsList.size() == 0)
			return null;

		final CrawlingStatistics averagedStats = new CrawlingStatistics();
		int crawlingAvgDurationCount = 0;

		for (final CrawlingStatistics stats : statsList) {

			if (stats.pfnOkCount != null) {
				averagedStats.pfnOkCount = initializeIfNull(averagedStats.pfnOkCount);
				averagedStats.pfnOkCount += stats.pfnOkCount;
			}

			if (stats.pfnInaccessibleCount != null) {
				averagedStats.pfnInaccessibleCount = initializeIfNull(averagedStats.pfnInaccessibleCount);
				averagedStats.pfnInaccessibleCount += stats.pfnInaccessibleCount;
			}

			if (stats.pfnCorruptCount != null) {
				averagedStats.pfnCorruptCount = initializeIfNull(averagedStats.pfnCorruptCount);
				averagedStats.pfnCorruptCount += stats.pfnCorruptCount;
			}

			if (stats.pfnUnknownStatusCount != null) {
				averagedStats.pfnUnknownStatusCount = initializeIfNull(averagedStats.pfnUnknownStatusCount);
				averagedStats.pfnUnknownStatusCount += stats.pfnUnknownStatusCount;
			}

			if (stats.fileSizeTotalBytes != null) {
				averagedStats.fileSizeTotalBytes = initializeIfNull(averagedStats.fileSizeTotalBytes);
				averagedStats.fileSizeTotalBytes += stats.fileSizeTotalBytes;
			}

			if (stats.crawlingAvgDurationMillis != null) {
				averagedStats.crawlingAvgDurationMillis = initializeIfNull(averagedStats.crawlingAvgDurationMillis);
				averagedStats.crawlingAvgDurationMillis += stats.crawlingAvgDurationMillis;
				crawlingAvgDurationCount += 1;
			}

			if (stats.pfnCount != null) {
				averagedStats.pfnCount = initializeIfNull(averagedStats.pfnCount);
				averagedStats.pfnCount += stats.pfnCount;
			}

			if (stats.crawlingTotalDurationMillis != null) {
				averagedStats.crawlingTotalDurationMillis = initializeIfNull(averagedStats.crawlingTotalDurationMillis);
				averagedStats.crawlingTotalDurationMillis += stats.crawlingTotalDurationMillis;
			}

			if (stats.downloadedPFNsTotalCount != null) {
				averagedStats.downloadedPFNsTotalCount = initializeIfNull(averagedStats.downloadedPFNsTotalCount);
				averagedStats.downloadedPFNsTotalCount += stats.downloadedPFNsTotalCount;
			}

			if (stats.downloadTotalDurationMillis != null) {
				averagedStats.downloadTotalDurationMillis = initializeIfNull(averagedStats.downloadTotalDurationMillis);
				averagedStats.downloadTotalDurationMillis += stats.downloadTotalDurationMillis;
			}

			if (stats.xrdfsPFNsTotalCount != null) {
				averagedStats.xrdfsPFNsTotalCount = initializeIfNull(averagedStats.xrdfsPFNsTotalCount);
				averagedStats.xrdfsPFNsTotalCount += stats.xrdfsPFNsTotalCount;
			}

			if (stats.xrdfsTotalDurationMillis != null) {
				averagedStats.xrdfsTotalDurationMillis = initializeIfNull(averagedStats.xrdfsTotalDurationMillis);
				averagedStats.xrdfsTotalDurationMillis += stats.xrdfsTotalDurationMillis;
			}
		}

		averagedStats.crawlingAvgDurationMillis = crawlingAvgDurationCount == 0 ? null : averagedStats.crawlingAvgDurationMillis / crawlingAvgDurationCount;
		averagedStats.statGeneratedUnixTimestamp = System.currentTimeMillis();

		return averagedStats;
	}

	/**
	 * @param value
	 * @return the value provided if it is not null, 0 otherwise
	 */
	private static Long initializeIfNull(final Long value) {
		return value == null ? 0L : value;
	}

	/**
	 * @param json
	 * @param key
	 * @return the value associated with the key from the json or null if the key is not found
	 */
	private static Long getLongFromJSON(final JSONObject json, final String key) {
		if (json.get(key) != null)
			return (Long) json.get(key);
		else
			return null;
	}

	@Override
	public String toString() {
		return toJSON(this).toJSONString();
	}
}
