package alien.io;


import java.util.Date;

import lazyj.DBFunctions;
import lia.util.StringFactory;

/**
 * Wrapper around one row in TRANSFERS_DIRECT
 * 
 * @author costing
 */
public class TransferDetails {

	/**
	 * transfer queue id
	 */
	int transferId;  
	
	/**
	 * priority
	 */
	int priority;
	
	/**
	 * change time
	 */
	Date ctime;   
	
	/**
	 * status
	 */
	String status;
	
	/**
	 * jdl
	 */
	String jdl;
	
	/**
	 * LFN
	 */
	String lfn;
	
	/**
	 * file size
	 */
	long size;  
	
	/**
	 * error code
	 */
	int error;
	
	/**
	 * started timestamp
	 */
	long started;      
	
	/**
	 * sent timestamp
	 */
	long sent;       
	
	/**
	 * finished timestamp
	 */
	long finished;     
	
	/**
	 * received timestamp
	 */
	long received;
	
	/**
	 * expires
	 */
	int expires;
	
	/**
	 * transfer group
	 */
	int transferGroup;
	
	/**
	 * transfer options
	 */
	String options;    
	
	/**
	 * target SE
	 */
	String destination;
	
	/**
	 * user who has submitted it 
	 */
	String user;         
	
	/**
	 * attempts
	 */
	int attempts;
	
	/**
	 * protocols
	 */
	String protocols;
	
	/**
	 * type
	 */
	String type;
	
	/**
	 * agent id
	 */
	int agentid;
	
	/**
	 * failure reason
	 */
	String reason;
	
	/**
	 * pfn
	 */
	String pfn;
	
	/**
	 * protocol ID
	 */
	String protocolid;
	
	/**
	 * FTD instance
	 */
	String ftd;
	
	/**
	 * ?!
	 */
	int persevere;
	
	/**
	 * retry time (?!)
	 */
	int retrytime;
	
	/**
	 * max time (?!)
	 */
	int maxtime;      
	
	/**
	 * @param db
	 */
	TransferDetails(final DBFunctions db){
		transferId = db.geti("transferId");
		priority = db.geti("priority");
		ctime = db.getDate("ctime");
		status = StringFactory.get(db.gets("status", null));
		jdl = db.gets("jdl", null);
		lfn = db.gets("lfn", null);
		size = db.getl("size");
		error = db.geti("error");
		started = db.getl("started");
		sent = db.getl("sent");
		finished = db.getl("finished");
		received = db.getl("received");
		expires = db.geti("expires");
		transferGroup = db.geti("transferGroup");
		options = StringFactory.get(db.gets("options", null));
		destination = StringFactory.get(db.gets("destination", null));
		user = StringFactory.get(db.gets("user", null));
		attempts = db.geti("attempts");
		protocols = StringFactory.get(db.gets("protocols", null));
		type = StringFactory.get(db.gets("type", null));
		agentid = db.geti("agentid");
		reason = db.gets("reason", null);
		pfn = db.gets("pfn", null);
		protocolid = StringFactory.get(db.gets("protocolid", null));
		ftd = StringFactory.get(db.gets("ftd", null));
		persevere = db.geti("persevere");
		retrytime = db.geti("retrytime");
		maxtime = db.geti("maxtime");
	}
	
}
