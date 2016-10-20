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

import org.apache.commons.lang3.StringUtils;

import com.orbitz.consul.KeyValueClient;

/**
 * Builder for {@link ConsulConfigurationSource}.
 */
public class ConsulConfigurationSourceBuilder {

  private String host;
  private int port;
  private String path;
  private KeyValueClient keyValueClient;

  /**
   * Construct {@link ConsulConfigurationSource}s builder
   * <p>
   * Default setup (override using with*() methods)
   * <ul>
   * <li>host: localhost</li>
   * <li>port: 8500</li>
   * </ul>
   */
  public ConsulConfigurationSourceBuilder() {
    host = "localhost";
    port = 8500;
    path = "/";
  }

  /**
   * Set Consul host for {@link ConsulConfigurationSource}s built by this builder.
   *
   * @param host host to use
   * @return this builder with Consul host set to provided parameter
   */
  public ConsulConfigurationSourceBuilder withHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Set Consul port for {@link ConsulConfigurationSource}s built by this builder.
   *
   * @param port port to use
   * @return this builder with Consul port set to provided parameter
   */
  public ConsulConfigurationSourceBuilder withPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * Set the Consul K-V key path for {@link ConsulConfigurationSource}s built by this builder.
   *
   * @param path Consul K-V root path
   * @return this builder with Consul port set to provided parameter
   */
  public ConsulConfigurationSourceBuilder withPath(String path) {
    this.path = path;
    return this;
  }

  public ConsulConfigurationSourceBuilder withKeyValueClient(KeyValueClient keyValueClient) {
    this.keyValueClient = keyValueClient;
    return this;
  }


  /**
   * Build a {@link ConsulConfigurationSource} using this builder's configuration
   *
   * @return new {@link ConsulConfigurationSource}
   */
  public ConsulConfigurationSource build() {
    boolean isPath = StringUtils.isNotEmpty(this.path);
    if (keyValueClient != null) {
      if (isPath) {
        return new ConsulConfigurationSource(this.keyValueClient, this.path);
      }
      return new ConsulConfigurationSource(this.keyValueClient);
    } else if (isPath) {
      return new ConsulConfigurationSource(host, port, path);
    } else {
      return new ConsulConfigurationSource(host, port);
    }
  }

  @Override
  public String toString() {
    return "ConsulConfigurationSource{" + "host=" + host + ", port=" + port + '}';
  }
}
