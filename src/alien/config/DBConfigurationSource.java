package alien.config;

import java.util.HashMap;
import java.util.Map;

import lazyj.DBFunctions;
import lazyj.DBProperties;
import lazyj.ExtProperties;

class DBConfigurationSource implements ConfigSource {
  private Map<String, ExtProperties> oldConfig;

  public DBConfigurationSource(final Map<String, ExtProperties> oldConfig) {
    this.oldConfig = oldConfig;
  }

  public Map<String, ExtProperties> getConfiguration() {
    Map<String, ExtProperties> dbConfig = new HashMap<String, ExtProperties>();
    dbConfig.put("config", getConfigFromDB(oldConfig.get("config")));
    return dbConfig;
  }

  private ExtProperties getConfigFromDB(final ExtProperties fileConfig) {
    ExtProperties tmp = new ExtProperties();

		if (isCentralService() && fileConfig.getb("jalien.config.hasDBBackend", true)) {
			final DBFunctions dbAdmin = new DBFunctions(oldConfig.get("admin"));


			if (dbAdmin != null) {
				final DBProperties dbProp = new DBProperties(dbAdmin);
        dbProp.makeReadOnly();
        tmp = dbProp;
			}
		}

    return tmp;
  }

  public boolean isCentralService() {
		for (final Map.Entry<String, ExtProperties> entry : oldConfig.entrySet()) {
			final ExtProperties prop = entry.getValue();

      if (prop.gets("driver").length() > 0 && prop.gets("password").length() > 0) {
        return true;
      }
		}

    return false;
  }
}
