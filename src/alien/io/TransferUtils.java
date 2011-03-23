package alien.io;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author costing
 *
 */
public final class TransferUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(TransferUtils.class.getCanonicalName());
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(TransferUtils.class.getCanonicalName());
	
	/**
	 * @return database connection to the transfers
	 */
	static DBFunctions getDB(){
		return ConfigUtils.getDB("transfers");
	}
	
	/**
	 * @param id
	 * @return the transfer with the given ID
	 */
	public static TransferDetails getTransfer(final int id){
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("TRANSFERS_db_lookup");
			monitor.incrementCounter("TRANSFERS_get_by_id");
		}
		
		db.query("SELECT * FROM TRANSFERS_DIRECT WHERE transferId="+id);
		
		if (!db.moveNext())
			return null;
		
		return new TransferDetails(db);
	}
	
	/**
	 * @param targetSE
	 * @return transfers to this SE
	 */
	public static List<TransferDetails> getActiveTransfersBySE(final String targetSE){
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;

		if (monitor!=null){
			monitor.incrementCounter("TRANSFERS_db_lookup");
			monitor.incrementCounter("TRANSFERS_get_by_destination");
		}
		
		db.query("SELECT * FROM TRANSFERS_DIRECT WHERE destination='"+Format.escSQL(targetSE)+"' ORDER BY transferId");
		
		final List<TransferDetails> ret = new ArrayList<TransferDetails>(db.count());
		
		while (db.moveNext()){
			ret.add(new TransferDetails(db));
		}
		
		return ret;
	}
	
	
	/**
	 * @param username
	 * @return transfers to this SE
	 */
	public static List<TransferDetails> getActiveTransfersByUser(final String username){
		final DBFunctions db = getDB();
		
		if (db==null)
			return null;
		
		if (monitor!=null){
			monitor.incrementCounter("TRANSFERS_db_lookup");
			monitor.incrementCounter("TRANSFERS_get_by_user");
		}
		
		String q = "SELECT * FROM TRANSFERS_DIRECT ";
		
		if (username!=null && username.length()>0)
			q += "WHERE user='"+Format.escSQL(username)+"' ";
			
		q += "ORDER BY transferId"; 
		
		db.query(q);
		
		final List<TransferDetails> ret = new ArrayList<TransferDetails>(db.count());
		
		while (db.moveNext()){
			ret.add(new TransferDetails(db));
		}
		
		return ret;
	}
}
