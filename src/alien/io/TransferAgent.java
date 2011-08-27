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
	
	/**
	 * 
	 */
	public TransferAgent() {
		super("TransferAgent");
		
		setDaemon(false);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		final TransferBroker broker = TransferBroker.getInstance();
		
		while (true){
			final Transfer t = broker.getWork();
			
			if (t!=null){
				logger.log(Level.INFO, "Performing transfer "+t.getTransferId());
				
				try{
					t.run();
				}
				catch (final Exception e){
					logger.log(Level.WARNING, "Transfer threw exception", e);
				}
				finally{
					logger.log(Level.INFO, "Transfer finished: "+t);
					
					broker.notifyTransferComplete(t);
				}
			}
			else{
				try{
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
		
		final List<TransferAgent> agents = new ArrayList<TransferAgent>(workers);
		
		for (int i=0; i<workers; i++){
			final TransferAgent ta = new TransferAgent();
			
			ta.start();
			
			agents.add(ta);
		}
	}
	
}
