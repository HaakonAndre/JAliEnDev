package alien.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lazyj.ExtProperties;
import lazyj.FallbackProperties;

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

		for (final Map.Entry<String, ExtProperties> entry : newConfiguration.entrySet()) {
			String name = entry.getKey();
			ExtProperties oldProp = cfgStorage.get(name);
			ExtProperties newProp = entry.getValue();

			ExtProperties merged = mergeProperties(oldProp, newProp, overwrite);
			cfgStorage.put(name, merged);
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

	public static ExtProperties mergeProperties(final ExtProperties a, final ExtProperties b, final boolean overwrite) {
		if (a == null && b == null) {
			return new ExtProperties();
		}
		else
			if (a != null && b == null) {
				return a;
			}
			else
				if (a == null && b != null) {
					return b;
				}

		if (a instanceof FallbackProperties) {
			FallbackProperties tmp = (FallbackProperties) a;
			tmp.addProvider(b, overwrite);
			return tmp;
		}
		else {
			FallbackProperties tmp = new FallbackProperties();
			tmp.addProvider(a);
			tmp.addProvider(b, overwrite);
			return tmp;
		}
	}

	public static ExtProperties mergeProperties(final ExtProperties a, final ExtProperties b) {
		return mergeProperties(a, b, false);
	}
}
