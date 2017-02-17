package alien.optimizers;

import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

/**
 * @author Miguel
 * @since Aug 9, 2016
 */
public class Optimizer extends Thread {

	static transient final Logger logger = ConfigUtils.getLogger(Optimizer.class.getCanonicalName());

	int sleep_period = 60 * 1000; // 1min

	String[] catalogue_optimizers = { "alien.optimizers.catalogue.LTables", "alien.optimizers.catalogue.GuidTable" };

	public void run() {
		this.run("all");
	}

	public void run(String type) {
		logger.log(Level.INFO, "Starting optimizers: " + type);

		if (!ConfigUtils.isCentralService()) {
			logger.log(Level.INFO, "We are not a central service :-( !");
			return;
		}

		switch (type) {
		case "catalogue":
			startCatalogueOptimizers();
			break;
		case "job":
			startJobOptimizers();
			break;
		case "transfer":
			startTransferOptimizers();
			break;
		default:
			startAllOptimizers();
		}
	}

	private void startAllOptimizers() {
		startCatalogueOptimizers();
		startJobOptimizers();
		startTransferOptimizers();
	}

	private void startCatalogueOptimizers() {
		for (String opt : catalogue_optimizers) {
			try {
				Optimizer optclass = (Optimizer) Class.forName(opt).newInstance();
				logger.log(Level.INFO, "New catalogue optimizer: " + opt);
				optclass.start();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				logger.log(Level.SEVERE, "Can't instantiate optimizer " + opt + "! " + e);
			}
		}
	}

	private void startJobOptimizers() {
	}

	private void startTransferOptimizers() {
	}

	public int getSleepPeriod() {
		return this.sleep_period;
	}

	public void setSleepPeriod(int newSleepPeriod) {
		this.sleep_period = newSleepPeriod;
	}
}
