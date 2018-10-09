package alien.config;

import java.util.Map;

import lazyj.ExtProperties;

public interface ConfigSource {
	public Map<String, ExtProperties> getConfiguration();
}
