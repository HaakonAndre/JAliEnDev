package alien.api.catalogue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.BookingTable;
import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;

/**
 * Register the output of a (failed) job
 *
 * @author costing
 * @since 2019-07-15
 */
/**
 * @author costing
 *
 */
public class RegisterOutput extends Request {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(RegisterOutput.class.getCanonicalName());

	private static final long serialVersionUID = -2004904530203513524L;
	private final long jobID;

	private Collection<LFN> registeredLFNs = null;

	/**
	 * @param user
	 * @param jobID
	 */
	public RegisterOutput(final AliEnPrincipal user, final long jobID) {
		setRequestUser(user);
		this.jobID = jobID;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(jobID));
	}

	@Override
	public void run() {
		registeredLFNs = BookingTable.registerOutput(getEffectiveRequester(), Long.valueOf(jobID));
	}

	/**
	 * @return the registered LFNs
	 */
	public Collection<LFN> getRegisteredLFNs() {
		return registeredLFNs;
	}
}
