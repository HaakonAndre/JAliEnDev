/**
 * 
 */
package alien.io;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.ExtProperties;

import alien.config.ConfigUtils;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class TransferAgent extends Thread {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(TransferAgent.class.getCanonicalName());
	
	private final int transferAgentID;
	
	/**
	 * 
	 */
	/**
	 * @param transferAgentID unique identifier
	 */
	public TransferAgent(final int transferAgentID) {
		super("TransferAgent "+transferAgentID);
		
		this.transferAgentID = transferAgentID;
		
		setDaemon(false);
	}
	
	/**
	 * @return this guy's ID
	 */
	int getTransferAgentID(){
		return transferAgentID;
	}
	
	private volatile Transfer work = null;
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		final TransferBroker broker = TransferBroker.getInstance();
		
		while (true){
			work = broker.getWork();
			
			if (work!=null){
				TransferBroker.touch(work, this);
				
				logger.log(Level.INFO, "Performing transfer "+work.getTransferId());
				
				try{
					work.run();
				}
				catch (final Exception e){
					logger.log(Level.WARNING, "Transfer threw exception", e);
				}
				finally{
					logger.log(Level.INFO, "Transfer finished: "+work);
					
					TransferBroker.notifyTransferComplete(work);
					
					work = null;
					
					TransferBroker.touch(null, this);
				}
			}
			else{
				try{
					logger.log(Level.INFO, "Agent "+transferAgentID+" : no work for me");
					
					Thread.sleep(1000*30);	// try in 30 seconds again to see if there is anything for it to do
				}
				catch (InterruptedException ie){
					// ignore
				}
			}
		}
	}
	
	/**
	 * Run the TransferAgent<br>
	 * <br>
	 * Configuration options:<br>
	 * alien.io.TransferAgent.workers = 5 (default)
	 * 
	 * @param args
	 */
	public static void main(String args[]){
		final ExtProperties config = alien.config.ConfigUtils.getConfig();
		
		final int workers = config.geti("alien.io.TransferAgent.workers", 5);
		
		logger.log(Level.INFO, "Starting "+workers+" workers");
		
		final List<TransferAgent> agents = new ArrayList<TransferAgent>(workers);
		
		for (int i=0; i<workers; i++){
			final TransferAgent ta = new TransferAgent(i);
			
			ta.start();
			
			agents.add(ta);
		}
		
		while (true){
			try{
				Thread.sleep(1000*30);
			}
			catch (Exception e){
				// ignore
			}
			
			for (TransferAgent ta: agents)
				TransferBroker.touch(ta.work, ta);
		}
	}
	
}
