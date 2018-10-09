package alien.config;

import java.util.HashMap;
import java.util.Map;

import lazyj.DBFunctions;
import lazyj.DBProperties;
import lazyj.ExtProperties;

class DBConfigurationSource implements ConfigSource {
	private Map<String, ExtProperties> oldConfig;
	private boolean isCentralService;

	public DBConfigurationSource(final Map<String, ExtProperties> oldConfig, boolean isCentralService) {
		this.oldConfig = oldConfig;
		this.isCentralService = isCentralService;
	}

	public Map<String, ExtProperties> getConfiguration() {
		Map<String, ExtProperties> dbConfig = new HashMap<String, ExtProperties>();
		dbConfig.put("config", getConfigFromDB(oldConfig.get("config")));
		return dbConfig;
	}

	private ExtProperties getConfigFromDB(final ExtProperties fileConfig) {
		ExtProperties tmp = new ExtProperties();

		if (isCentralService && fileConfig.getb("jalien.config.hasDBBackend", true)) {
			final DBFunctions dbAdmin = new DBFunctions(oldConfig.get("admin"));

			if (dbAdmin != null) {
				final DBProperties dbProp = new DBProperties(dbAdmin);
				dbProp.makeReadOnly();
				tmp = dbProp;
			}
		}

		return tmp;
	}
}
