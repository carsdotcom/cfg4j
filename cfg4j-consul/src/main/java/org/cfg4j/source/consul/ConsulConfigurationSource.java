/*
 * Copyright 2015-2016 Norbert Potocki (norbert.potocki@nort.pl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.cfg4j.source.consul;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.SourceCommunicationException;
import org.cfg4j.source.context.environment.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.kv.Value;

/**
 * Note: use {@link ConsulConfigurationSourceBuilder} for building instances of this class.
 * <p>
 * Read configuration from the Consul K-V store.
 */
class ConsulConfigurationSource implements ConfigurationSource {

  private static final Logger LOG = LoggerFactory.getLogger(ConsulConfigurationSource.class);

  private KeyValueClient kvClient;
  private Map<String, String> consulValues;
  private final String host;
  private final int port;
  private final String path;
  private boolean initialized;

  /**
   * Note: use {@link ConsulConfigurationSourceBuilder} for building instances of this class.
   * <p>
   * Read configuration from the Consul K-V store located at {@code host}:{@code port}.
   *
   * @param host Consul host to connect to
   * @param port Consul port to connect to
   */
  ConsulConfigurationSource(String host, int port) {
    this(host, port, "/");
  }

  /**
   * Note: use {@link ConsulConfigurationSourceBuilder} for building instances of this class.
   * <p>
   * Read configuration from the Consul K-V store located at {@code host}:{@code port}.
   *
   * @param host Consul host to connect to
   * @param port Consul port to connect to
   * @param keyRoot Consul K-V store root
   */
  ConsulConfigurationSource(String host, int port, String keyRoot) {
    this.host = requireNonNull(host);
    this.port = port;
    this.path = keyRoot;
    initialized = false;
  }

  /**
   * Note: use {@link ConsulConfigurationSourceBuilder} for building instances of this class.
   * <p>
   * Read configuration from the Consul K-V store using the supplied {@code KeyValueClient}.
   *
   * @param kvClient {@code KeyValueClient} to talk to Consul
   */
  ConsulConfigurationSource(KeyValueClient keyValueClient) {
    this(keyValueClient, "/");
  }

  /**
   * Note: use {@link ConsulConfigurationSourceBuilder} for building instances of this class.
   * <p>
   * Read configuration from the Consul K-V store using the supplied {@code KeyValueClient}.
   *
   * @param kvClient {@code KeyValueClient} to talk to Consul
   * @param path key root to be used to read from Consul
   */
  ConsulConfigurationSource(KeyValueClient kvClient, String path) {
    this.kvClient = requireNonNull(kvClient, "kvClient is required");
    this.path = path;

    // Not required; here because of the contract
    this.port = 8500;
    this.host = "localhost";
    initialized = true;

  }

  @Override
  public Properties getConfiguration(Environment environment) {
    LOG.trace("Requesting configuration for environment: " + environment.getName());

    if (!initialized) {
      throw new IllegalStateException(
          "Configuration source has to be successfully initialized before you request configuration.");
    }

    reload();

    Properties properties = new Properties();
    String path = environment.getName();

    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    if (path.length() > 0 && !path.endsWith("/")) {
      path = path + "/";
    }
    for (Map.Entry<String, String> entry : consulValues.entrySet()) {
      if (entry.getKey().startsWith(path)) {
        String key = entry.getKey().substring(path.length()).replace("/", ".");
        properties.put(key, entry.getValue());
      }
    }

    return properties;
  }

  /**
   * @throws SourceCommunicationException when unable to connect to Consul client
   */
  @Override
  public void init() {
    if (!initialized) {
      try {
        LOG.info("Connecting to Consul client at " + host + ":" + port);

        Consul consul = Consul.builder().withHostAndPort(HostAndPort.fromParts(host, port))
            .withReadTimeoutMillis(20000).build();

        kvClient = consul.keyValueClient();
      } catch (Exception e) {
        throw new SourceCommunicationException("Can't connect to host " + host + ":" + port, e);
      }
      initialized = true;
    }
  }

  private void reload() {
    Map<String, String> newConsulValues = new HashMap<>();
    List<Value> valueList;

    try {
      LOG.debug("Reloading configuration from Consuls' K-V store");
      valueList = kvClient.getValues(this.path);
    } catch (Exception e) {
      initialized = false;
      throw new SourceCommunicationException("Can't get values from k-v store", e);
    }

    for (Value value : valueList) {
      String val = "";

      if (value.getValueAsString().isPresent()) {
        val = value.getValueAsString().get();
      }

      LOG.trace("Consul provided configuration key: " + value.getKey() + " with value: " + val);

      newConsulValues.put(value.getKey(), val);
    }

    consulValues = newConsulValues;
  }

  @Override
  public String toString() {
    return "ConsulConfigurationSource{" + "consulValues=" + consulValues + ", kvClient=" + kvClient
        + '}';
  }

}
