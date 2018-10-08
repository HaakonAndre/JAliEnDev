package alien.test.unit;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import alien.config.ConfigManager;
import alien.config.ConfigSource;

import lazyj.ExtProperties;

class ConfigUtilsTests {
  private class MockConfigSource implements ConfigSource {
    private Map<String, ExtProperties> cfgStorage;

    public MockConfigSource() {
      cfgStorage = new HashMap<String, ExtProperties>();
    }

    public void set(String file, String key, String value) {
      if(!cfgStorage.containsKey(file)) {
        cfgStorage.put(file, new ExtProperties());
      }

      cfgStorage.get(file).set(key, value);
    }

    public Map<String, ExtProperties> getConfiguration() {
      return cfgStorage;
    }
  }

  @Test
  void testSingleSource() {
    MockConfigSource src = new MockConfigSource();
    ConfigManager cfgManager = new ConfigManager();

    src.set("config", "key", "a");
    cfgManager.registerPrimary(src);

    String read = cfgManager.getConfiguration().get("config").gets("key");
    Assertions.assertEquals("a", read);
  }

  @Test
  void testMergeProperties() {
    ExtProperties front = new ExtProperties();
    ExtProperties back = new ExtProperties();

    front.set("key", "a");
    back.set("key", "b");

    ExtProperties merged;
    String read;

    merged = ConfigManager.mergeProperties(front, back);
    read = merged.gets("key");
    Assertions.assertEquals("a", read);

    merged = ConfigManager.mergeProperties(back, front);
    read = merged.gets("key");
    Assertions.assertEquals("b", read);
  }

  @Test
  void testMergePropertiesUpdate() {
    ExtProperties front = new ExtProperties();
    ExtProperties back = new ExtProperties();

    front.set("otherkey", "c");
    back.set("key", "b");

    ExtProperties merged;
    String read;

    merged = ConfigManager.mergeProperties(front, back);
    front.set("key", "a");
    read = merged.gets("key");
    Assertions.assertEquals("a", read);
  }

  @Test
  void testReloadingConfigurationsSingleSource() {
    String read;
    MockConfigSource src = new MockConfigSource();
    src.set("config", "key", "a");

    ConfigManager cfgManager = new ConfigManager();
    cfgManager.registerPrimary(src);
    read = cfgManager.getConfiguration().get("config").gets("key");
    Assertions.assertEquals("a", read);

    src.set("config", "key", "b");
    read = cfgManager.getConfiguration().get("config").gets("key");
    Assertions.assertEquals("b", read);
  }

  @Test
  void testReloadConfigurationsMultipleSources() {
    String read;
    MockConfigSource srcA = new MockConfigSource();
    MockConfigSource srcB = new MockConfigSource();

    srcA.set("config", "otherkey", "a");
    srcB.set("config", "key", "b");

    ConfigManager cfgManager = new ConfigManager();
    cfgManager.registerPrimary(srcA);
    cfgManager.registerFallback(srcB);

    read = cfgManager.getConfiguration().get("config").gets("key");
    Assertions.assertEquals("b", read);

    srcA.set("config", "key", "a");
    read = cfgManager.getConfiguration().get("config").gets("key");
    Assertions.assertEquals("a", read);
  }

  @Test
  void testReloadConfigurationPrimary() {
    Assertions.fail("not implemented");
  }

  @Test
  void testReloadConfigurationFallback() {
    Assertions.fail("not implemented");
  }
}
