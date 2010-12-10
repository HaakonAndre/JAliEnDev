package alien.catalogue.access;

import java.io.Serializable;

/**
 * Generic envelope
 * 
 * @author costing
 */
public class AccessTicket implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6200985047035616911L;

	/**
	 * Access type
	 */
	public final AccessType type;
	
	/**
	 * @param type
	 */
	public AccessTicket(final AccessType type){
		this.type = type;
	}	
}
