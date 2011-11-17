/**
 * 
 */
package alien.io;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.DBFunctions.DBConnection;
import lazyj.Format;
import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import apmon.ApMon;

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
		
		cleanup();
		
		final DBConnection dbc = db.getConnection();
		
		executeQuery(dbc, "lock tables TRANSFERS_DIRECT write, PROTOCOLS read, active_transfers read;");
		executeQuery(dbc, "select transferId,lfn,destination from TRANSFERS_DIRECT inner join PROTOCOLS on (sename=destination) where status='WAITING' and (SELECT count(1) FROM active_transfers WHERE se_name=sename)<max_transfers order by transferId asc limit 1;");
	
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
		
		logger.log(Level.FINE, transferId+" : LFN is "+lfn);
		
		if (lfn.guid==null){
			logger.log(Level.WARNING, "GUID '"+lfn.guid+"' is null for transfer ID "+transferId+", lfn '"+sLFN+"'");
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "GUID is null for this LFN");
			return null;
		}
		
		final GUID guid = GUIDUtils.getGUID(lfn);

		if (guid==null){
			logger.log(Level.WARNING, "GUID '"+lfn.guid+"' doesn't exist in the catalogue for transfer ID "+transferId+", lfn '"+sLFN+"'");
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "GUID was not found in the database");
			return null;
		}

		guid.lfnCache = new HashSet<LFN>();
		guid.lfnCache.add(lfn);
		
		logger.log(Level.FINE, transferId+" : GUID is "+guid);
				
		final SE se = SEUtils.getSE(targetSE);
		
		if (se==null){
			logger.log(Level.WARNING, "Target SE '"+targetSE+"' doesn't exist for transfer ID "+transferId);
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "Target SE doesn't exist");
			return null;
		}
		
		logger.log(Level.FINE, transferId+" : Target SE is "+se);
		
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
				
		for (final PFN source: pfns){
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
		
		logger.log(Level.FINE, transferId+" : booked PFN is "+target);
		
		final Transfer t = new Transfer(transferId, pfns, target);
		
		reportMonitoring(t);
		
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
			case Transfer.FAILED_UNKNOWN:
				return "FAILED";
			case Transfer.FAILED_SYSTEM:
				return "KILLED";
			case Transfer.DELAYED:
				return "WAITING";
			default:
				return "TRANSFERRING";
		}		
	}
	
	private static final int getAliEnTransferStatus(final int exitCode){
		switch (exitCode){
			case Transfer.OK:
				return 7;
			case Transfer.FAILED_SOURCE:
				return -1;
			case Transfer.FAILED_TARGET:
				return -1;
			case Transfer.FAILED_UNKNOWN:
				return -1;
			case Transfer.FAILED_SYSTEM:
				return -2;
			case Transfer.DELAYED:
				return -3;
			default:
				return 5;	// transferring
		}		
	}
	
	private static long lastCleanedUp = 0; 
	
	private static synchronized void cleanup(){
		if (System.currentTimeMillis() - lastCleanedUp<1000*30)
			return;
		
		lastCleanedUp = System.currentTimeMillis();
		
		try{
			final DBFunctions db = ConfigUtils.getDB("transfers");
			
			if (db==null)
				return;
			
			db.query("DELETE FROM active_transfers WHERE last_active<"+((lastCleanedUp/1000) - 90));
			
			db.query("UPDATE TRANSFERS_DIRECT SET status='EXPIRED', finished="+(lastCleanedUp/1000)+", reason='TransferAgent no longer active' WHERE status='TRANSFERRING' AND transferId NOT IN (SELECT transfer_id FROM active_transfers);");
		}
		catch (Throwable t){
			logger.log(Level.SEVERE, "Exception cleaning up", t);
		}
	}
	
	/**
	 * Mark a transfer as active
	 * 
	 * @param t
	 * @param ta
	 */
	public static void touch(final Transfer t, final TransferAgent ta) {
		try {
			final DBFunctions db = ConfigUtils.getDB("transfers");

			if (db == null)
				return;

			if (t == null) {
				db.query("DELETE FROM active_transfers WHERE transfer_agent_id=" + ta.getTransferAgentID() + " AND pid=" + MonitorFactory.getSelfProcessID() + " AND host='"
						+ Format.escSQL(MonitorFactory.getSelfHostname()) + "'");
				return;
			}

			final Map<String, Object> values = new HashMap<String, Object>();

			values.put("last_active", Long.valueOf(System.currentTimeMillis() / 1000));
			values.put("se_name", SEUtils.getSE(t.target.seNumber).seName);
			values.put("transfer_id", Integer.valueOf(t.getTransferId()));
			values.put("transfer_agent_id", Integer.valueOf(ta.getTransferAgentID()));
			values.put("pid", MonitorFactory.getSelfHostname());
			values.put("host", MonitorFactory.getSelfHostname());
			
			if (t.lastTriedSE>0)
				values.put("active_source", SEUtils.getSE(t.lastTriedSE).seName);
			else
				values.put("active_source", "");
			
			if (t.lastTriedProtocol!=null)
				values.put("active_protocol", t.lastTriedProtocol.toString());
			else
				values.put("active_protocol", "");

			db.query(DBFunctions.composeUpdate("active_transfers", values, Arrays.asList("transfer_agent_id", "pid", "host")));

			if (db.getUpdateCount() == 0)
				db.query(DBFunctions.composeInsert("active_transfers", values));
		}
		catch (Throwable ex) {
			logger.log(Level.SEVERE, "Exception updating status", ex);
		}
	}
	
	private static boolean markTransfer(final int transferId, final int exitCode, final String reason){
		final DBFunctions db = ConfigUtils.getDB("transfers");
		
		if (db==null)
			return false;

		String formattedReason = reason;
		
		if (formattedReason!=null && formattedReason.length()>250)
			formattedReason = formattedReason.substring(0, 250);
		
		db.query("update TRANSFERS_DIRECT set status='"+getTransferStatus(exitCode)+"', reason='"+Format.escSQL(formattedReason)+"', finished="+(System.currentTimeMillis()/1000)+" WHERE transferId="+transferId+" AND status='TRANSFERRING'");
		
		if (db.getUpdateCount()<1)
			return false;
		
		db.query("update PROTOCOLS set current_transfers=greatest(coalesce(current_transfers,0)-1,0) WHERE sename=(SELECT destination FROM TRANSFERS_DIRECT WHERE transferId="+transferId+");");
		
		return true;
	}
	
	private static final void reportMonitoring(final Transfer t) {
		try {
			final ApMon apmon;

			try {
				final Vector<String> targets = new Vector<String>();
				targets.add(ConfigUtils.getConfig().gets("CS_ApMon", "aliendb4.cern.ch"));

				apmon = new ApMon(targets);
			}
			catch (Exception e) {
				logger.log(Level.WARNING, "Could not initialize apmon", e);
				return;
			}

			final String cluster = "TransferQueue_Transfers_" + ConfigUtils.getConfig().gets("Organization", "ALICE");

			final Vector<String> p = new Vector<String>();
			final Vector<Object> v = new Vector<Object>();

			p.add("statusID");
			v.add(Integer.valueOf(getAliEnTransferStatus(t.getExitCode())));

			p.add("size");
			v.add(Double.valueOf(t.sources.iterator().next().getGuid().size));

			p.add("started");
			v.add(Double.valueOf(t.started / 1000));

			if (t.getExitCode() >= Transfer.OK) {
				p.add("finished");
				v.add(Double.valueOf(System.currentTimeMillis() / 1000));

				if (t.lastTriedSE > 0) {
					SE se = SEUtils.getSE(t.lastTriedSE);

					if (se != null) {
						p.add("SE");
						v.add(se.seName);
					}
				}
				
				if (t.lastTriedProtocol!=null){
					p.add("Protocol");
					v.add(t.lastTriedProtocol.toString());
				}
			}

			p.add("destination");
			v.add(SEUtils.getSE(t.target.seNumber).seName);

			p.add("user");
			v.add(t.target.getGuid().owner);

			try {
				apmon.sendParameters(cluster, String.valueOf(t.getTransferId()), p.size(), p, v);
			}
			catch (Exception e) {
				logger.log(Level.WARNING, "Could not send apmon message: "+p+" -> "+v, e);
			}
		}
		catch (Throwable ex) {
			logger.log(Level.WARNING, "Exception reporting the monitoring", ex);
		}
	}

	/**
	 * When a transfer has completed, call this method to update the database status
	 * 
	 * @param t
	 */
	public static void notifyTransferComplete(final Transfer t) {
		// TODO : verify the storage reply envelope here

		markTransfer(t.getTransferId(), t.getExitCode(), t.getFailureReason());

		reportMonitoring(t);

		if (t.getExitCode() == Transfer.OK) {
			// Update the file catalog with the new replica
			final AliEnPrincipal admin = UserFactory.getByUsername("monalisa");

			BookingTable.commit(admin, t.target);
		}
	}
}
