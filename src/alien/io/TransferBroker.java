/**
 * 
 */
package alien.io;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.DBFunctions.DBConnection;
import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;

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
			logger.log(Level.WARNING, "Exception executing the query", e);
		}
	}
	
	/**
	 * @return the next transfer to be performed, or <code>null</code> if there is nothing to do
	 */
	public synchronized Transfer getWork(){
		final DBFunctions db = ConfigUtils.getDB("transfers");
		
		if (db==null){
			logger.log(Level.WARNING, "Could not connect to the transfers database");
			
			return null;
		}
		
		final DBConnection dbc = db.getConnection();
		
		executeQuery(dbc, "lock tables TRANSFERS_DIRECT write, PROTOCOLS write;");
		executeQuery(dbc, "select transferId,lfn,destination from TRANSFERS_DIRECT inner join PROTOCOLS on (sename=destination) where status='WAITING' and current_transfers<max_transfers order by transferId asc limit 1;");
	
		int transferId = -1;
		String sLFN = null;
		String targetSE = null;
		
		try{
			if (resultSet!=null && resultSet.next()){
				transferId = resultSet.getInt(1);
				sLFN = resultSet.getString(2);
				targetSE = resultSet.getString(3);
			}
			else{
				logger.log(Level.INFO, "There is no waiting transfer in the queue");
				return null;
			}
			
			if (transferId<0 || sLFN==null || sLFN.length()==0 || targetSE==null || targetSE.length()==0){
				logger.log(Level.INFO, "Transfer details are wrong");
				return null;
			}

			executeQuery(dbc, "update TRANSFERS_DIRECT set status='TRANSFERRING' where transferId="+transferId+";");
			executeQuery(dbc, "update PROTOCOLS set current_transfers=current_transfers+1 WHERE sename='"+Format.escSQL(targetSE)+"'");
		}
		catch (Exception e){
			logger.log(Level.WARNING, "Exception fetching data from the query", e);
			// ignore
		}
		finally{
			executeQuery(dbc, "unlock tables;");
			executeClose();
			
			dbc.free();
		}
		
		final LFN lfn = LFNUtils.getLFN(sLFN);
		
		if (!lfn.exists){
			logger.log(Level.WARNING, "LFN '"+sLFN+"' doesn't exist in the catalogue for transfer ID "+transferId);
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "LFN doesn't exist in the catalogue");
			return null;
		}
		
		if (lfn.guid==null){
			logger.log(Level.WARNING, "GUID '"+lfn.guid+"' is null for transfer ID "+transferId+", lfn '"+sLFN+"'");
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "GUID is null for this LFN");
			return null;
		}
		
		final GUID guid = GUIDUtils.getGUID(lfn.guid);
		
		if (guid==null){
			logger.log(Level.WARNING, "GUID '"+lfn.guid+"' doesn't exist in the catalogue for transfer ID "+transferId+", lfn '"+sLFN+"'");
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "GUID was not found in the database");
			return null;
		}
		
		final SE se = SEUtils.getSE(targetSE);
		
		if (se==null){
			logger.log(Level.WARNING, "Target SE '"+targetSE+"' doesn't exist for transfer ID "+transferId);
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "Target SE doesn't exist");
			return null;
		}
		
		final Set<PFN> pfns = lfn.whereisReal();
		
		if (pfns==null || pfns.size()==0){
			logger.log(Level.WARNING, "No existing replicas to mirror for transfer ID "+transferId);
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "No replicas to mirror");
			return null;
		}
		
		for (final PFN pfn: pfns){
			if (pfn.seNumber == se.seNumber){
				logger.log(Level.WARNING, "There already exists a replica of '"+sLFN+"' on '"+targetSE+"' for transfer ID "+transferId);
				markTransfer(transferId, Transfer.FAILED_SYSTEM, "There is already a replica on this storage");
				return null;
			}
		}
		
		final StringTokenizer st = new StringTokenizer(targetSE, ":");
		
		st.nextToken();
		final String site = st.nextToken();
		
		final List<PFN> sortedPFNs = SEUtils.sortBySite(pfns, site, false);
		
		for (final PFN source: sortedPFNs){
			final String reason = AuthorizationFactory.fillAccess(source, AccessType.READ);
		
			if (reason!=null){
				logger.log(Level.WARNING, "Could not obtain source authorization for transfer ID "+transferId+" : "+reason);
				markTransfer(transferId, Transfer.FAILED_SYSTEM, "Source authorization failed: "+reason);
				return null;
			}
		}
				
		final PFN target;
		
		try{
			target = BookingTable.bookForWriting(lfn, guid, null, 0, se);
		}
		catch (IOException ioe){
			final String reason = ioe.getMessage();
			logger.log(Level.WARNING, "Could not obtain target authorization for transfer ID "+transferId+" : "+reason);
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "Target authorization failed: "+reason);
			return null;
		}
		
		final Transfer t = new Transfer(transferId, sortedPFNs, target);
		
		return t;
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
	
	private static void markTransfer(final int transferId, final int exitCode, final String reason){
		final DBFunctions db = ConfigUtils.getDB("transfers");
		
		if (db==null)
			return;

		String formattedReason = reason;
		
		if (formattedReason!=null && formattedReason.length()>250)
			formattedReason = formattedReason.substring(0, 250);
		
		db.query("update TRANSFERS_DIRECT set status='"+getTransferStatus(exitCode)+"', reason='"+Format.escSQL(formattedReason)+"' WHERE transferId="+transferId);
	}
	
	/**
	 * When a transfer has completed, call this method to update the database status
	 * 
	 * @param t 
	 */
	public static void notifyTransferComplete(final Transfer t){
		// TODO : verify the storage reply envelope here
		
		markTransfer(t.getTransferId(), t.getExitCode(), t.getFailureReason());
		
		if (t.getExitCode() == Transfer.OK){
			// Update the file catalog with the new replica
			final AliEnPrincipal admin = UserFactory.getByUsername("monalisa");
			
			BookingTable.commit(admin, t.target);
		}
	}
}
