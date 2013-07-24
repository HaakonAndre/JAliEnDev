/**
 * 
 */
package alien.io;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import lazyj.ExtProperties;
import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;

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

	private final int pid = MonitorFactory.getSelfProcessID();
	
	private final String hostname = MonitorFactory.getSelfHostname();
	
	/**
	 * 
	 */
	/**
	 * @param transferAgentID
	 *            unique identifier
	 */
	public TransferAgent(final int transferAgentID) {
		super("TransferAgent " + transferAgentID);

		this.transferAgentID = transferAgentID;

		setDaemon(false);
	}

	/**
	 * @return this guy's ID
	 */
	int getTransferAgentID() {
		return transferAgentID;
	}
	
	int getPID(){
		return pid;
	}
	
	String getHostName(){
		return hostname;
	}

	private volatile Transfer work = null;

	private boolean shouldStop = false;

	private void signalStop() {
		shouldStop = true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		try {
			final TransferBroker broker = TransferBroker.getInstance();

			boolean firstTimeNoWork = true;

			while (!shouldStop) {
				work = broker.getWork(this);

				if (work != null) {
					TransferBroker.touch(work, this);

					logger.log(Level.INFO, "Performing transfer " + work.getTransferId());

					try {
						work.run();
					}
					catch (final Exception e) {
						logger.log(Level.WARNING, "Transfer threw exception", e);
					}
					finally {
						logger.log(Level.INFO, "Transfer finished: " + work);

						TransferBroker.notifyTransferComplete(work);

						work = null;

						TransferBroker.touch(null, this);
					}

					firstTimeNoWork = true;
				}
				else {
					try {
						if (firstTimeNoWork) {
							logger.log(Level.INFO, "Agent " + transferAgentID + " : no work for me");
							firstTimeNoWork = false;
						}

						Thread.sleep(1000 * 30); // try in 30 seconds again to
													// see if there is anything
													// for it to do
					}
					catch (final InterruptedException ie) {
						// ignore
					}
				}
			}
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Exiting after an exception", e);
		}
	}

	private static int transferAgentIDSequence = 0;

	/**
	 * Run the TransferAgent<br>
	 * <br>
	 * Configuration options:<br>
	 * alien.io.TransferAgent.workers = 5 (default)
	 * 
	 * @param args
	 */
	public static void main(final String args[]) {
		final ExtProperties config = alien.config.ConfigUtils.getConfig();

		int workers = config.geti("alien.io.TransferAgent.workers", 5);

		if (workers < 0 || workers > 100) // typo ?!
			workers = 5;

		logger.log(Level.INFO, "Starting " + workers + " workers");

		final LinkedList<TransferAgent> agents = new LinkedList<TransferAgent>();

		for (int i = 0; i < workers; i++) {
			final TransferAgent ta = new TransferAgent(transferAgentIDSequence++);

			ta.start();

			agents.add(ta);
		}

		while (true) {
			try {
				Thread.sleep(1000 * 30);

				workers = config.geti("alien.io.TransferAgent.workers", workers);

				if (workers < 0 || workers > 100) // typo ?!
					workers = 5;

				final Iterator<TransferAgent> it = agents.iterator();

				while (it.hasNext()) {
					final TransferAgent agent = it.next();
					if (!agent.isAlive()) {
						logger.log(Level.SEVERE, "One worker is no longer alive, removing the respective agent from the list: " + agent.getName());

						it.remove();
					}
				}

				while (workers > agents.size()) {
					final TransferAgent ta = new TransferAgent(transferAgentIDSequence++);

					ta.start();

					agents.add(ta);
				}

				while (agents.size() > workers) {
					final TransferAgent ta = agents.removeLast();

					ta.signalStop();
				}
			}
			catch (final Exception e) {
				// ignore
			}

			for (final TransferAgent ta : agents)
				TransferBroker.touch(ta.work, ta);
		}
	}

}
