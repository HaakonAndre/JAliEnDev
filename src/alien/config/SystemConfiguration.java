package alien.config;

import java.util.HashMap;
import java.util.Map;

import lazyj.ExtProperties;

public class SystemConfiguration implements ConfigSource {
  public Map<String, ExtProperties> getConfiguration() {
    Map<String, ExtProperties> tmp = new HashMap<String, ExtProperties>();
    tmp.put("config", new ExtProperties(System.getProperties()));
    return tmp;
  }
}
