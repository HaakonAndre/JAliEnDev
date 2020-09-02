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
	 * The percentage of PFNs that are ok from the total number of PFNs crawled
	 */
	long pfnOkCount;

	/**
	 * The percentage of PFNs that are inaccessible from the total number of PFNs crawled
	 */
	long pfnInaccessibleCount;

	/**
	 * The percentage of PFNs that are corrupt from the total number of PFNs crawled
	 */
	long pfnCorruptCount;

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

	private static final int DEFAULT_VALUE = -1;

	private CrawlingStatistics() {
		this.pfnCount = 0;
		this.pfnOkCount = 0;
		this.pfnInaccessibleCount = 0;
		this.pfnCorruptCount = 0;
		this.crawlingMinDurationMillis = Long.MAX_VALUE;
		this.crawlingMaxDurationMillis = Long.MAX_VALUE;
		this.crawlingAvgDurationMillis = 0;
		this.crawlingTotalDurationMillis = 0;
		this.iterationTotalDurationMillis = 0;
	}

	CrawlingStatistics(long pfnCount, long pfnOkCount, long pfnInaccessibleCount, long pfnCorruptCount, long crawlingMinDurationMillis, long crawlingMaxDurationMillis, long crawlingAvgDurationMillis, long crawlingTotalDurationMillis, long iterationTotalDurationMillis) {
		this.pfnCount = pfnCount;
		this.pfnOkCount = pfnOkCount;
		this.pfnInaccessibleCount = pfnInaccessibleCount;
		this.pfnCorruptCount = pfnCorruptCount;
		this.crawlingMinDurationMillis = crawlingMinDurationMillis;
		this.crawlingMaxDurationMillis = crawlingMaxDurationMillis;
		this.crawlingAvgDurationMillis = crawlingAvgDurationMillis;
		this.crawlingTotalDurationMillis = crawlingTotalDurationMillis;
		this.iterationTotalDurationMillis = iterationTotalDurationMillis;
	}

	static CrawlingStatistics fromJSON(JSONObject jsonObject) {
		return new CrawlingStatistics(
				getLongFromJSON(jsonObject, "pfnCount"),
				getLongFromJSON(jsonObject, "pfnOkCount"),
				getLongFromJSON(jsonObject, "pfnInaccessibleCount"),
				getLongFromJSON(jsonObject, "pfnCorruptCount"),
				getLongFromJSON(jsonObject, "crawlingMinDurationMillis"),
				getLongFromJSON(jsonObject, "crawlingMaxDurationMillis"),
				getLongFromJSON(jsonObject, "crawlingAvgDurationMillis"),
				getLongFromJSON(jsonObject, "crawlingTotalDurationMillis"),
				getLongFromJSON(jsonObject, "iterationTotalDurationMillis")
		);
	}

	@SuppressWarnings("unchecked")
	static JSONObject toJSON(CrawlingStatistics stats) {

		JSONObject json = new JSONObject();

		json.put("pfnCount", getValueForJSON(stats.pfnCount));
		json.put("pfnOkCount", getValueForJSON(stats.pfnOkCount));
		json.put("pfnInaccessibleCount", getValueForJSON(stats.pfnInaccessibleCount));
		json.put("pfnCorruptCount", getValueForJSON(stats.pfnCorruptCount));
		json.put("crawlingMinDurationMillis", getValueForJSON(stats.crawlingMinDurationMillis));
		json.put("crawlingMaxDurationMillis", getValueForJSON(stats.crawlingMaxDurationMillis));
		json.put("crawlingAvgDurationMillis", getValueForJSON(stats.crawlingAvgDurationMillis));
		json.put("crawlingTotalDurationMillis", getValueForJSON(stats.crawlingTotalDurationMillis));
		json.put("iterationTotalDurationMillis", getValueForJSON(stats.iterationTotalDurationMillis));

		return json;
	}

	private static Long getValueForJSON(long value) {
		return  value != DEFAULT_VALUE ? Long.valueOf(value) : null;
	}

	static CrawlingStatistics getAveragedStats(List<CrawlingStatistics> statsList) {

		if (statsList == null || statsList.size() == 0)
			return null;

		CrawlingStatistics averagedStats = new CrawlingStatistics();

		int pfnOkCount = 0, pfnInaccessibleCount = 0, pfnCorruptCount = 0, crawlingAvgDurationCount = 0;

		for (CrawlingStatistics stats : statsList) {

			if (stats.crawlingMinDurationMillis != DEFAULT_VALUE && stats.crawlingMinDurationMillis < averagedStats.crawlingMinDurationMillis)
				averagedStats.crawlingMinDurationMillis = stats.crawlingMinDurationMillis;

			if (stats.crawlingMaxDurationMillis != DEFAULT_VALUE && stats.crawlingMaxDurationMillis > averagedStats.crawlingMaxDurationMillis)
				averagedStats.crawlingMaxDurationMillis = stats.crawlingMaxDurationMillis;

			if (stats.iterationTotalDurationMillis != DEFAULT_VALUE && stats.iterationTotalDurationMillis > averagedStats.iterationTotalDurationMillis)
				averagedStats.iterationTotalDurationMillis = stats.iterationTotalDurationMillis;

			if (stats.pfnOkCount != DEFAULT_VALUE) {
				averagedStats.pfnOkCount += stats.pfnOkCount;
				pfnOkCount++;
			}

			if (stats.pfnInaccessibleCount != DEFAULT_VALUE) {
				averagedStats.pfnInaccessibleCount += stats.pfnInaccessibleCount;
				pfnInaccessibleCount++;
			}

			if (stats.pfnCorruptCount != DEFAULT_VALUE) {
				averagedStats.pfnCorruptCount += stats.pfnCorruptCount;
				pfnCorruptCount++;
			}

			if (stats.crawlingAvgDurationMillis != DEFAULT_VALUE) {
				averagedStats.crawlingAvgDurationMillis += stats.crawlingAvgDurationMillis;
				crawlingAvgDurationCount++;
			}

			averagedStats.pfnCount += stats.pfnCount == DEFAULT_VALUE ? 0 : stats.pfnCount;
			averagedStats.crawlingTotalDurationMillis += stats.crawlingTotalDurationMillis == DEFAULT_VALUE ? 0 : stats.crawlingTotalDurationMillis;
		}

		averagedStats.crawlingAvgDurationMillis = crawlingAvgDurationCount == 0 ? DEFAULT_VALUE : averagedStats.crawlingAvgDurationMillis / crawlingAvgDurationCount;
		averagedStats.pfnOkCount = pfnOkCount == 0 ? DEFAULT_VALUE : averagedStats.pfnOkCount / pfnOkCount;
		averagedStats.pfnInaccessibleCount = pfnInaccessibleCount == 0 ? DEFAULT_VALUE : averagedStats.pfnInaccessibleCount / pfnInaccessibleCount;
		averagedStats.pfnCorruptCount = pfnCorruptCount == 0 ? DEFAULT_VALUE : averagedStats.pfnCorruptCount / pfnCorruptCount;

		return averagedStats;
	}

	private static long getLongFromJSON(JSONObject json, String key) {
		try {
			return ((Long) json.get(key)).longValue();
		}
		catch (Exception e) {
			e.printStackTrace();
			return DEFAULT_VALUE;
		}
	}

	@Override
	public String toString() {
		return toJSON(this).toJSONString();
	}
}
