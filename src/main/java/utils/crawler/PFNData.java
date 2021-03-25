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

	/**
	 * @param guid
	 * @param seNumber
	 * @param pfn
	 * @param observedSize
	 * @param catalogueSize
	 * @param observedMD5
	 * @param catalogueMD5
	 * @param downloadDurationMillis
	 * @param xrdfsDurationMillis
	 * @param statusCode
	 * @param statusType
	 * @param statusMessage
	 * @param timestamp
	 */
	public PFNData(final String guid, final Integer seNumber, final String pfn, final Long observedSize, final Long catalogueSize, final String observedMD5, final String catalogueMD5,
			final Long downloadDurationMillis, final Long xrdfsDurationMillis, final String statusCode, final String statusType, final String statusMessage, final Long timestamp) {
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

	private void setData(final Map<String, Object> data) {
		this.data = data;
	}

	/**
	 * @return
	 */
	public String toCSV() {
		final Collection<Object> values = data.values();
		return values.stream().map(Object::toString).collect(Collectors.joining(","));
	}

	/**
	 * @return
	 */
	public String getCsvHeader() {
		final Set<String> keys = data.keySet();
		return String.join(",", keys);
	}

	/**
	 * @return
	 */
	public Collection<Object> getValues() {
		return data.values();
	}

	/**
	 * @return
	 */
	public Map<String, Object> getData() {
		return Collections.unmodifiableMap(data);
	}

	/**
	 * @return
	 */
	public String getStatusCode() {
		try {
			return data.get("statusCode").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public String getSeNumber() {
		try {
			return data.get("seNumber").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public String getStatusType() {
		try {
			return data.get("statusType").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public String getStatusMessage() {
		try {
			return data.get("statusMessage").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public String getPfn() {
		try {
			return data.get("pfn").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public String getGuid() {
		try {
			return data.get("guid").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public Long getObservedSize() {
		try {
			return Long.valueOf(this.data.get("observedSize").toString());
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public Long getCatalogueSize() {
		try {
			return Long.valueOf(this.data.get("catalogueSize").toString());
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public String getObservedMD5() {
		try {
			return this.data.get("observedMD5").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public String getCatalogueMD5() {
		try {
			return this.data.get("catalogueMD5").toString();
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public Long getDownloadDurationMillis() {
		try {
			return Long.valueOf(this.data.get("downloadDurationMillis").toString());
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public Long getXrdfsDurationMillis() {
		try {
			return Long.valueOf(this.data.get("xrdfsDurationMillis").toString());
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	public Long getOutputGeneratedTimestamp() {
		try {
			return Long.valueOf(this.data.get("timestamp").toString());
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return null;
		}
	}

	/**
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public JSONObject toJSON() {
		final JSONObject json = new JSONObject();
		json.putAll(data);
		return json;
	}

	/**
	 * @param jsonObject
	 * @return
	 */
	public static PFNData fromJSON(final JSONObject jsonObject) {
		final PFNData pfnData = new PFNData();
		final Map<String, Object> data = new LinkedHashMap<>();

		for (final Object key : jsonObject.keySet()) {
			data.put(key.toString(), jsonObject.get(key));
		}

		pfnData.setData(data);

		return pfnData;
	}

	@Override
	public String toString() {
		return this.toJSON().toJSONString();
	}
}
