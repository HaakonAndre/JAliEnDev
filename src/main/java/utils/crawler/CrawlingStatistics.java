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
	public Integer pfnCount;

	/**
	 * The percentage of PFNs that are ok from the total number of PFNs crawled
	 */
	public Integer pfnOkPct;

	/**
	 * The percentage of PFNs that are inaccessible from the total number of PFNs crawled
	 */
	public Integer pfnInaccessiblePct;

	/**
	 * The percentage of PFNs that are corrupt from the total number of PFNs crawled
	 */
	public Integer pfnCorruptPct;

	/**
	 * Minimum duration in milliseconds of the crawling process for one single PFN
	 */
	public Long crawlingMinDurationMillis;

	/**
	 * Maximum duration in milliseconds of the crawling process for one single PFN
	 */
	public Long crawlingMaxDurationMillis;

	/**
	 * Average duration in milliseconds of the crawling process
	 */
	public Long crawlingAvgDurationMillis;

	/**
	 * Total duration in milliseconds of the crawling process
	 */
	public Long crawlingTotalDurationMillis;

	/**
	 * Total duration in milliseconds of the current iteration
	 */
	public Long iterationTotalDurationMillis;

	public CrawlingStatistics(Integer pfnCount, Integer pfnOkPct, Integer pfnInaccessiblePct, Integer pfnCorruptPct, Long crawlingMinDurationMillis,
	                          Long crawlingMaxDurationMillis, Long crawlingAvgDurationMillis, Long crawlingTotalDurationMillis, Long iterationTotalDurationMillis) {
		this.pfnCount = pfnCount;
		this.pfnOkPct = pfnOkPct;
		this.pfnInaccessiblePct = pfnInaccessiblePct;
		this.pfnCorruptPct = pfnCorruptPct;
		this.crawlingMinDurationMillis = crawlingMinDurationMillis;
		this.crawlingMaxDurationMillis = crawlingMaxDurationMillis;
		this.crawlingAvgDurationMillis = crawlingAvgDurationMillis;
		this.crawlingTotalDurationMillis = crawlingTotalDurationMillis;
		this.iterationTotalDurationMillis = iterationTotalDurationMillis;
	}

	public static CrawlingStatistics fromJSON(JSONObject jsonObject) {
		return new CrawlingStatistics(
				getIntegerFromJSON(jsonObject, "pfnCount"),
				getIntegerFromJSON(jsonObject, "pfnOkPct"),
				getIntegerFromJSON(jsonObject, "pfnInaccessiblePct"),
				getIntegerFromJSON(jsonObject, "pfnCorruptPct"),
				getLongFromJSON(jsonObject, "crawlingMinDurationMillis"),
				getLongFromJSON(jsonObject, "crawlingMaxDurationMillis"),
				getLongFromJSON(jsonObject, "crawlingAvgDurationMillis"),
				getLongFromJSON(jsonObject, "crawlingTotalDurationMillis"),
				getLongFromJSON(jsonObject, "iterationTotalDurationMillis")
		);
	}

	@SuppressWarnings("unchecked")
	public static JSONObject toJSON(CrawlingStatistics stats) {

		JSONObject json = new JSONObject();

		json.put("pfnCount", stats.pfnCount);
		json.put("pfnOkPct", stats.pfnOkPct);
		json.put("pfnInaccessiblePct", stats.pfnInaccessiblePct);
		json.put("pfnCorruptPct", stats.pfnCorruptPct);
		json.put("crawlingMinDurationMillis", stats.crawlingMinDurationMillis);
		json.put("crawlingMaxDurationMillis", stats.crawlingMaxDurationMillis);
		json.put("crawlingAvgDurationMillis", stats.crawlingAvgDurationMillis);
		json.put("crawlingTotalDurationMillis", stats.crawlingTotalDurationMillis);
		json.put("iterationTotalDurationMillis", stats.iterationTotalDurationMillis);

		return json;
	}

	public static CrawlingStatistics getAveragedStats(List<CrawlingStatistics> statsList) {

		if (statsList == null || statsList.size() == 0)
			return null;

		CrawlingStatistics averagedStats = new CrawlingStatistics(0, 0, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0L, 0L, 0L);

		Integer pfnOkCount = 0, pfnInaccessibleCount = 0, pfnCorruptCount = 0, crawlingAvgDurationCount = 0;

		for (CrawlingStatistics stats : statsList) {

			if (stats.crawlingMinDurationMillis != null && stats.crawlingMinDurationMillis < averagedStats.crawlingMinDurationMillis)
				averagedStats.crawlingMinDurationMillis = stats.crawlingMinDurationMillis;

			if (stats.crawlingMaxDurationMillis != null && stats.crawlingMaxDurationMillis > averagedStats.crawlingMaxDurationMillis)
				averagedStats.crawlingMaxDurationMillis = stats.crawlingMaxDurationMillis;

			if (stats.iterationTotalDurationMillis != null && stats.iterationTotalDurationMillis > averagedStats.iterationTotalDurationMillis)
				averagedStats.iterationTotalDurationMillis = stats.iterationTotalDurationMillis;

			if (stats.pfnOkPct != null) {
				averagedStats.pfnOkPct += stats.pfnOkPct;
				pfnOkCount++;
			}

			if (stats.pfnInaccessiblePct != null) {
				averagedStats.pfnInaccessiblePct += stats.pfnInaccessiblePct;
				pfnInaccessibleCount++;
			}

			if (stats.pfnCorruptPct != null) {
				averagedStats.pfnCorruptPct += stats.pfnCorruptPct;
				pfnCorruptCount++;
			}

			if (stats.crawlingAvgDurationMillis != null) {
				averagedStats.crawlingAvgDurationMillis += stats.crawlingAvgDurationMillis;
				crawlingAvgDurationCount++;
			}

			averagedStats.pfnCount += stats.pfnCount == null ? 0 : stats.pfnCount;
			averagedStats.crawlingTotalDurationMillis += stats.crawlingTotalDurationMillis == null ? 0 : stats.crawlingTotalDurationMillis;
		}

		averagedStats.crawlingAvgDurationMillis = crawlingAvgDurationCount == 0 ? null : averagedStats.crawlingAvgDurationMillis / crawlingAvgDurationCount;
		averagedStats.pfnOkPct = pfnOkCount == 0 ? null : averagedStats.pfnOkPct / pfnOkCount;
		averagedStats.pfnInaccessiblePct = pfnInaccessibleCount == 0 ? null : averagedStats.pfnInaccessiblePct / pfnInaccessibleCount;
		averagedStats.pfnCorruptPct = pfnCorruptCount == 0 ? null : averagedStats.pfnCorruptPct / pfnCorruptCount;

		return averagedStats;
	}

	public static Integer getIntegerFromJSON(JSONObject json, String key) {
		try {
			return ((Long) json.get(key)).intValue();
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static Long getLongFromJSON(JSONObject json, String key) {
		try {
			return (Long) json.get(key);
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public String toString() {
		return toJSON(this).toJSONString();
	}
}
