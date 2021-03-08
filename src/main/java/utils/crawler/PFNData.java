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

	private void setData(Map<String, Object> data) {
		this.data = data;
	}

	public String toCSV() {
		Collection<Object> values = data.values();
		return values.stream().map(Object::toString).collect(Collectors.joining(","));
	}

	public String getCsvHeader() {
		Set<String> keys = data.keySet();
		return String.join(",", keys);
	}

	public Collection<Object> getValues() {
		return data.values();
	}

	public Map<String, Object> getData() {
		return Collections.unmodifiableMap(data);
	}

	public String getStatusCode() {
		try {
			return data.get("statusCode").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	public String getSeNumber() {
		try {
			return data.get("seNumber").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	public String getStatusType() {
		try {
			return data.get("statusType").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	public String getStatusMessage() {
		try {
			return data.get("statusMessage").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	public String getPfn() {
		try {
			return data.get("pfn").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	public String getGuid() {
		try {
			return data.get("guid").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	public Long getObservedSize() {
		try {
			return Long.parseLong(this.data.get("observedSize").toString());
		}
		catch (Exception e) {
			return null;
		}
	}

	public Long getCatalogueSize() {
		try {
			return Long.parseLong(this.data.get("catalogueSize").toString());
		}
		catch (Exception e) {
			return null;
		}
	}

	public String getObservedMD5() {
		try {
			return this.data.get("observedMD5").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	public String getCatalogueMD5() {
		try {
			return this.data.get("catalogueMD5").toString();
		}
		catch (Exception e) {
			return null;
		}
	}

	public Long getDownloadDurationMillis() {
		try {
			return Long.parseLong(this.data.get("downloadDurationMillis").toString());
		}
		catch (Exception e) {
			return null;
		}
	}

	public Long getXrdfsDurationMillis() {
		try {
			return Long.parseLong(this.data.get("xrdfsDurationMillis").toString());
		}
		catch (Exception e) {
			return null;
		}
	}

	public Long getOutputGeneratedTimestamp() {
		try {
			return Long.parseLong(this.data.get("timestamp").toString());
		}
		catch (Exception e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public JSONObject toJSON() {
		JSONObject json = new JSONObject();
		json.putAll(data);
		return json;
	}

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
