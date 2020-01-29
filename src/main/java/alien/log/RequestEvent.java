package alien.log;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import alien.monitoring.Timing;
import alien.user.AliEnPrincipal;
import lazyj.Format;

/**
 * @author costing
 * @since 2020-01-29
 */
public class RequestEvent implements Closeable {

	/**
	 * Requester identity, if known
	 */
	public AliEnPrincipal identity = null;

	/**
	 * Remote IP address where the request came from
	 */
	public InetAddress clientAddress = null;

	/**
	 * Client site mapping, if known
	 */
	public String site = null;

	/**
	 * Unique session ID
	 */
	public UUID clientID = null;

	/**
	 * Command that was run
	 */
	public String command = null;

	/**
	 * Arguments to it
	 */
	public List<String> arguments = null;

	/**
	 * If some exception happened during running
	 */
	public Exception exception = null;

	/**
	 * Command exit code
	 */
	public int exitCode = Integer.MIN_VALUE;

	/**
	 * Any error message?
	 */
	public String errorMessage = null;

	/**
	 * Duration of this request
	 */
	private Timing timing = new Timing();

	private final OutputStream os;

	/**
	 * Create a request event that can be written to the given stream at the end of the execution
	 * 
	 * @param os
	 */
	public RequestEvent(final OutputStream os) {
		this.os = os;
	}

	private Map<String, Object> getValues() {
		final Map<String, Object> values = new LinkedHashMap<>();

		if (identity != null) {
			values.put("user", identity.getDefaultUser());
			values.put("role", identity.getDefaultRole());
		}

		if (clientAddress != null)
			values.put("address", clientAddress.getHostAddress());

		if (site != null)
			values.put("site", site);

		if (clientID != null)
			values.put("clientID", clientID).toString();

		if (command != null)
			values.put("command", command);

		if (arguments != null && arguments.size() > 0)
			values.put("arguments", arguments);

		if (exitCode != Integer.MIN_VALUE)
			values.put("exitCode", Integer.valueOf(exitCode));

		if (errorMessage != null)
			values.put("errorMessage", errorMessage);

		if (exception != null) {
			values.put("exceptionMessage", exception.getMessage());
			values.put("exceptionTrace", exception.getStackTrace());
		}

		values.put("duration", Double.valueOf(timing.getMillis()));

		return values;
	}

	/**
	 * @return the JSON representation of this event
	 */
	public String toJSON() {
		return Format.toJSON(getValues(), false).toString();
	}

	@Override
	public void close() throws IOException {
		if (os != null)
			os.write((toJSON() + "\n").getBytes());
	}
}
