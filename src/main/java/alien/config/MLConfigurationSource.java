package alien.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import lazyj.ExtProperties;

import lia.Monitor.monitor.AppConfig;

public class MLConfigurationSource implements ConfigSource {
	public Map<String, ExtProperties> getConfiguration() {
		Map<String, ExtProperties> tmp = new HashMap<String, ExtProperties>();
		tmp.put("config", getConfigFromML());
		return tmp;
	}

	private ExtProperties getConfigFromML() {
		// TODO: remove duplicated code here and in ConfigUtils!
		final String mlConfigURL = System.getProperty("lia.Monitor.ConfigURL");
		final boolean hasMLConfig = mlConfigURL != null && mlConfigURL.trim().length() > 0;

		ExtProperties tmp = new ExtProperties();

		if (hasMLConfig) {
			// assume running as a library inside ML code, inherit the configuration keys from its main config file
			final Properties mlConfigProperties = AppConfig.getPropertiesConfigApp();

			for (final String key : mlConfigProperties.stringPropertyNames())
				tmp.set(key, mlConfigProperties.getProperty(key));

			tmp.set("jalien.configure.logging", "false");
		}

		return tmp;
	}

}
