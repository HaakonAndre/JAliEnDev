package alien.ui;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import alien.catalogue.access.AuthorizationFactory;
import alien.user.AliEnPrincipal;

/**
 * @author costing
 * @since 2011-03-04
 */
public abstract class Request implements Serializable, Runnable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8044096743871226167L;

	/**
	 * Unique identifier of the VM, for communication purposes
	 */
	private static final UUID VM_UUID = UUID.randomUUID();
	
	/**
	 * Sequence number, for dispatching asynchronous messages
	 */
	private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);
	
	/**
	 * @return this VM's unique identifier
	 */
	public static final UUID getVMID(){
		return VM_UUID;
	}
	
	/**
	 * @return current sequence number (number of requests created since VM startup)
	 */
	public static final Long getCurrentSequenceNumber(){
		return Long.valueOf(ID_SEQUENCE.get());
	}
	
	/**
	 * Unique identifier of the VM that made the request
	 */
	private final UUID vm_uuid = VM_UUID;
	
	/**
	 * Request ID in the VM
	 */
	private final Long requestID = Long.valueOf(ID_SEQUENCE.incrementAndGet());
	
	/**
	 * The default identity of the VM
	 */
	private final AliEnPrincipal requester_uid = AuthorizationFactory.getDefaultUser();
	
	/**
	 * Effective identity (the user on behalf of whom the request came)
	 */
	private AliEnPrincipal requester_euid = requester_uid;
	
	/**
	 * Set on receiving a request over the network
	 */
	private transient AliEnPrincipal partner_identity = null;
	
	/**
	 * Set on receiving a request over the network
	 */
	private transient InetAddress partner_address = null;
	
	/**
	 * @return the unique identifier of the VM that generated the request
	 */
	public final UUID getVMUUID(){
		return vm_uuid;
	}
	
	/**
	 * @return sequence number within the VM that generated the request
	 */
	public final Long getRequestID(){
		return requestID;
	}
	
	/**
	 * @return requester identity (default identity of the VM)
	 */
	public final AliEnPrincipal getRequesterIdentity(){
		return requester_uid;
	}
	
	/**
	 * @return effective user on behalf of whom the request is executed
	 */
	public final AliEnPrincipal getEffectiveRequester(){
		return requester_euid;
	}
	
	/**
	 * @return identity of the partner, set on receiving a request over the wire
	 */
	public final AliEnPrincipal getPartnerIdentity(){
		return partner_identity;
	}
	
	/**
	 * @param id identity of the partner. This is called on receiving a request over the wire.
	 */
	public final void setPartnerIdentity(final AliEnPrincipal id){
		if (partner_identity!=null)
			throw new IllegalAccessError("You are not allowed to overwrite this field!");
		
		partner_identity = id;
	}
	
	/**
	 * @return partner's IP address
	 */
	public InetAddress getPartnerAddress() {
		return partner_address;
	}
	
	/**
	 * @param ip partner's address
	 */
	public final void setPartnerAddress(final InetAddress ip){
		if (this.partner_address!=null)
			throw new IllegalAccessError("You are not allowed to overwrite this field!");
		
		this.partner_address = ip;
	}
}
