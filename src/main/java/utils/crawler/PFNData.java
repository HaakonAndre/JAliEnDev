package utils.crawler;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.simple.JSONObject;

/**
 * Models the data that is gathered during the crawling process for every PFN
 *
 * @author anegru
 */
public class PFNData {
	private Map<String, Object> data = new LinkedHashMap<>();

	public PFNData(String guid, Integer seNumber, String pfn, Long observedSize, Long catalogueSize, String observedMD5, String catalogueMD5,
		Long downloadDurationMillis, Long xrdfsDurationMillis, String statusCode, String statusType, String statusMessage, Long timestamp) {
		data.put("guid", guid);
		data.put("seNumber", seNumber);
		data.put("pfn", pfn);
		data.put("observedSize", observedSize);
		data.put("catalogueSize", catalogueSize);
		data.put("observedMD5", observedMD5);
		data.put("catalogueMD5", catalogueMD5);
		data.put("downloadDurationMillis", downloadDurationMillis);
		data.put("xrdfsDurationMillis", xrdfsDurationMillis);
		data.put("statusCode", statusCode);
		data.put("statusType", statusType);
		data.put("statusMessage", statusMessage);
		data.put("timestamp", timestamp);
	}

	private PFNData() {
	}

	/**
	 * Set a map holding key-value pairs representing crawling properties
	 * @param data
	 */
	private void setData(Map<String, Object> data) {
		this.data = data;
	}

	/**
	 * @return format PFNData object as string in CSV format
	 */
	public String toCSV() {
		Collection<Object> values = data.values();
		return values.stream().map(Object::toString).collect(Collectors.joining(","));
	}

	/**
	 * @return comma separated list of all keys stored in the object
	 */
	public String getCsvHeader() {
		Set<String> keys = data.keySet();
		return String.join(",", keys);
	}

	/**
	 *
	 * @return all values stored in the object
	 */
	public Collection<Object> getValues() {
		return data.values();
	}

	/**
	 *
	 * @return the map that is used internally to hold key-value pairs
	 */
	public Map<String, Object> getData() {
		return Collections.unmodifiableMap(data);
	}

	/**
	 *
	 * @return the status code of the crawled PFN
	 */
	public String getStatusCode() {
		try {
			return data.get("statusCode").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 *
	 * @return the SE number on which the PFN analysed resides
	 */
	public String getSeNumber() {
		try {
			return data.get("seNumber").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 *
	 * @return the status type of the crawled PFN
	 */
	public String getStatusType() {
		try {
			return data.get("statusType").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 *
	 * @return the status message of the crawled PFN
	 */
	public String getStatusMessage() {
		try {
			return data.get("statusMessage").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 *
	 * @return the crawled PFN as string
	 */
	public String getPfn() {
		try {
			return data.get("pfn").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 *
	 * @return the GUID of the PFN
	 */
	public String getGuid() {
		try {
			return data.get("guid").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 * @return the size in bytes of the PFN after download
	 */
	public Long getObservedSize() {
		try {
			return Long.parseLong(this.data.get("observedSize").toString());
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 * @return the size in bytes of the PFN registered in the catalogue
	 */
	public Long getCatalogueSize() {
		try {
			return Long.parseLong(this.data.get("catalogueSize").toString());
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 * @return the MD5 of the PFN recomputed after download
	 */
	public String getObservedMD5() {
		try {
			return this.data.get("observedMD5").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 * @return the MD5 of the PFN registered in the catalogue
	 */
	public String getCatalogueMD5() {
		try {
			return this.data.get("catalogueMD5").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 * @return the total duration in milliseconds of the PFN download
	 */
	public Long getDownloadDurationMillis() {
		try {
			return Long.parseLong(this.data.get("downloadDurationMillis").toString());
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 *
	 * @return the total duration in milliseconds of the xrdfs call
	 */
	public Long getXrdfsDurationMillis() {
		try {
			return Long.parseLong(this.data.get("xrdfsDurationMillis").toString());
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 *
	 * @return the output generation timestamp
	 */
	public Long getOutputGeneratedTimestamp() {
		try {
			return Long.parseLong(this.data.get("timestamp").toString());
		}
		catch (Exception e) {
			return null;
		}
	}

	/**
	 * Convert the object to JSONObject
	 * @return JSONObject
	 */
	@SuppressWarnings("unchecked")
	public JSONObject toJSON() {
		JSONObject json = new JSONObject();
		json.putAll(data);
		return json;
	}

	/**
	 * Convert from a JSONObject to PFNData
	 * @param jsonObject
	 * @return PFNData
	 */
	public static PFNData fromJSON(JSONObject jsonObject) {
		PFNData pfnData = new PFNData();
		Map<String, Object> data = new LinkedHashMap<>();

		for (Object key : jsonObject.keySet()) {
			data.put(key.toString(), jsonObject.get(key));
		}

		pfnData.setData(data);

		return pfnData;
	}

	@Override
	@SuppressWarnings("unchecked")
	public String toString() {
		return this.toJSON().toJSONString();
	}
}
