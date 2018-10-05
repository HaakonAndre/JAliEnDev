package alien.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lazyj.ExtProperties;

public class ConfigManager implements ConfigSource {
  private Map<String, ExtProperties> cfgStorage;

  public ConfigManager() {
    cfgStorage = new HashMap<String, ExtProperties>();
  }

  public void registerPrimary(ConfigSource cfgSource) {
    registerSource(cfgSource, true);
  }

  public void registerFallback(ConfigSource cfgSource) {
    registerSource(cfgSource, false);
  }

  private void registerSource(ConfigSource cfgSource, boolean overwrite) {
    Map<String, ExtProperties> newConfiguration = cfgSource.getConfiguration();

    for(final Map.Entry<String, ExtProperties> entry: newConfiguration.entrySet()) {
      String key = entry.getKey();

      ExtProperties oldProp = cfgStorage.get(key);
      ExtProperties newProp = entry.getValue();
      ExtProperties merged;

      if(overwrite) {
        merged = mergeProperties(oldProp, newProp);
      } else {
        merged = mergeProperties(newProp, oldProp);
      }

      cfgStorage.put(key, merged);
    }
  }

  public Map<String, ExtProperties> getConfiguration() {
    return cfgStorage;
  }

  public void makeReadonly() {
		for (final Map.Entry<String, ExtProperties> entry : cfgStorage.entrySet()) {
			final ExtProperties prop = entry.getValue();
      prop.makeReadOnly();
		}

    cfgStorage = Collections.unmodifiableMap(cfgStorage);
  }

  public static ExtProperties mergeProperties(final ExtProperties a, final ExtProperties b) {
    ExtProperties tmp = new ExtProperties(a.getProperties());

    for (final Map.Entry<Object, Object> entry : b.getProperties().entrySet())
    	tmp.set(entry.getKey().toString(), entry.getValue().toString());

    return tmp;
  }
}
