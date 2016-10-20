package org.cfg4j.source.consul;

import java.util.Map;
import java.util.Properties;

import org.cfg4j.source.reload.ReloadStrategy;
import org.cfg4j.source.reload.Reloadable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.cache.ConsulCache.Listener;
import com.orbitz.consul.cache.KVCache;
import com.orbitz.consul.model.kv.Value;

/**
 * A reload strategy for Consul which listens for changes using Consul's blocking HTTP Api.
 * 
 * @author deepmistry
 *
 */
public class ConsulKVReloadStrategy implements ReloadStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(ConsulKVReloadStrategy.class);

  private KeyValueClient client;

  private KVCache cache;

  private String path;

  /**
   * Constructs the ConsulKVReloadStrategy
   * 
   * @param client {@link KeyValueClient} to be used to access Consul's K-V store
   * @param path listen for changes to this path in the Consul K-V store
   */
  public ConsulKVReloadStrategy(KeyValueClient client, String path) {
    this.client = client;
    this.path = path;
  }

  @Override
  public void register(final Reloadable resource) {
    cache = KVCache.newCache(client, path);

    cache.addListener(new Listener<String, Value>() {

      @Override
      public void notify(Map<String, Value> newValues) {
        resource.reload(convert(newValues));
      }

    });

    try {
      cache.start();
    } catch (Exception e) {
      LOG.error("Unable to start Consul's K-V listener", e);
    }

  }

  /*
   * Convert Map of Consul's values to a properties object
   */
  private Properties convert(Map<String, Value> consulValues) {
    Properties properties = new Properties();
    for (Map.Entry<String, Value> entry : consulValues.entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue().getValueAsString().isPresent()
          ? entry.getValue().getValueAsString().get() : "");
    }
    return properties;
  }

  @Override
  public void deregister(Reloadable resource) {
    try {
      cache.stop();
    } catch (Exception e) {
      LOG.error("Unable to stop Consul's K-V listener", e);
    }
  }

}
