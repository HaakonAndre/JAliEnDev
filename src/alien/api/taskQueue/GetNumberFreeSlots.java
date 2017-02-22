package alien.api.taskQueue;

import java.util.List;

import alien.api.Request;
import alien.taskQueue.JobBroker;
import alien.user.AliEnPrincipal;

/**
 * Get number of slots for a CE
 * 
 * @author mmmartin
 * @since Feb 20, 2017
 */
public class GetNumberFreeSlots extends Request {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5445861914172987654L;

	private List<Integer> slots;
	private String ceName;
	private String host;
	private Integer port;
	private String version;

	/**
	 * @param user
	 * @param role
	 * @param ce
	 */
	public GetNumberFreeSlots(final AliEnPrincipal user, final String role, final String host, final Integer port, final String ce, final String version) {
		setRequestUser(user);
		setRoleRequest(role);
		this.host = host;
		this.port = port;
		this.ceName = ce;
		this.version = version;
	}

	@Override
	public void run() {
		this.slots = JobBroker.getNumberFreeSlots(host, port, ceName, version);
	}

	/**
	 * @return code and job slots
	 */
	public List<Integer> getJobSlots() {
		return this.slots;
	}

	@Override
	public String toString() {
		return "Asked for number of free slots for host: " + host + " CE: " + ceName + ": " + this.slots.toString();
	}
}
