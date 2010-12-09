/**
 * 
 */
package alien.io;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.DBFunctions.DBConnection;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * @author costing
 * @since Dec 9, 2010
 */
public class TransferBroker {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(TransferBroker.class.getCanonicalName());
	
	private TransferBroker(){
		// just hide it
	}
	
	private static TransferBroker instance = null;
	
	/**
	 * @return singleton
	 */
	public static synchronized TransferBroker getInstance(){
		if (instance==null){
			instance = new TransferBroker();
		}
		
		return instance;
	}
	
	private ResultSet resultSet = null;
	
	private Statement stat = null;
	
	private final void executeClose(){
		if (resultSet!=null){
			try{
				resultSet.close();
			}catch (Throwable t){
				// ignore
			}
			
			resultSet = null;
		}
		
		if (stat!=null){
			try{
				stat.close();
			}
			catch (Throwable t){
				// ignore
			}
			
			stat = null;
		}
	}
	
	private final void executeQuery(final DBConnection dbc, final String query){
		executeClose();
		
		try{
			stat = dbc.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
			if (stat.execute(query, Statement.NO_GENERATED_KEYS)){
				resultSet = stat.getResultSet();
			}
			else{
				executeClose();
			}
		}
		catch (SQLException e){
			// ignore
		}
	}
	
	/**
	 * @return the next transfer to be performed, or <code>null</code> if there is nothing to do
	 */
	public Transfer getWork(){
		final DBFunctions db = ConfigUtils.getDB("transfers");
		
		if (db==null)
			return null;
		
		final DBConnection dbc = db.getConnection();
		
		executeQuery(dbc, "lock tables TRANSFERS_DIRECT write;");
		executeQuery(dbc, "select transferId,lfn,destination from TRANSFERS_DIRECT where status='WAITING' order by transferId asc limit 1;");
	
		int transferId = -1;
		String sLFN = null;
		String targetSE = null;
		
		try{
			if (resultSet!=null && resultSet.next()){
				transferId = resultSet.getInt(1);
				sLFN = resultSet.getString(2);
				targetSE = resultSet.getString(3);
			}
		}
		catch (Exception e){
			// ignore
		}
		
		if (transferId<0){
			executeClose();
			return null;
		}
		
		executeQuery(dbc, "update TRANSFERS_DIRECT where status='TRANSFERRING' order by transferId asc limit 1;");
		executeQuery(dbc, "unlock tables;");
		
		dbc.free();
		
		final LFN lfn = LFNUtils.getLFN(sLFN);
		
		if (!lfn.exists){
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "LFN doesn't exist in the catalogue");
			return null;
		}
		
		final SE se = SEUtils.getSE(targetSE);
		
		if (se==null){
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "Target SE doesn't exist");
			return null;
		}
		
		final Set<PFN> pfns = lfn.whereisReal();
		
		if (pfns==null || pfns.size()==0){
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "No replicas to mirror");
			return null;
		}
		
		for (final PFN pfn: pfns){
			if (pfn.seNumber == se.seNumber){
				markTransfer(transferId, Transfer.FAILED_SYSTEM, "There is already a replica on this storage");
				return null;
			}
		}
		
		// TODO : generate target PFN
		// TODO : register the PFN in the booking table
		// TODO : figure out the closest SE to copy from
		// TODO : generate access envelopes for both endpoints 
		
		return null;
	}
	
	private static final String getTransferStatus(final int exitCode){
		switch (exitCode){
			case Transfer.OK:
				return "DONE";
			case Transfer.FAILED_SOURCE:
				return "FAILED";
			case Transfer.FAILED_TARGET:
				return "FAILED";
			case Transfer.FAILED_SYSTEM:
				return "KILLED";
			case Transfer.DELAYED:
				return "WAITING";
			default:
				return "TRANSFERRING";
		}		
	}
	
	private void markTransfer(final int transferId, final int exitCode, final String reason){
		final DBFunctions db = ConfigUtils.getDB("transfers");
		
		if (db==null)
			return;
		
		db.query("update TRANSFERS_DIRECT set status='"+getTransferStatus(exitCode)+"', reason='"+Format.escSQL(reason)+"' WHERE transferId="+transferId);
	}
}
