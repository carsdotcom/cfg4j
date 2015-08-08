/*
 * Copyright 2015 Norbert Potocki (norbert.potocki@nort.pl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cfg4j.provider;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.NoSuchElementException;

/**
 * Invocation handler for proxies created by {@link ConfigurationProvider#bind(String, Class)}. Uses provided
 * {@link ConfigurationProvider} for getting properties.
 */
class BindInvocationHandler implements InvocationHandler {

  private final ConfigurationProvider simpleConfigurationProvider;
  private final String prefix;

  /**
   * Create invocation handler which fetches property from given {@code configurationProvider} using call to
   * {@link ConfigurationProvider#getProperty(String, Class)} method.
   *
   * @param configurationProvider configuration provider to use for fetching properties
   * @param prefix                prefix for calls to {@link ConfigurationProvider#getProperty(String, Class)}
   */
  BindInvocationHandler(ConfigurationProvider configurationProvider, String prefix) {
    this.simpleConfigurationProvider = requireNonNull(configurationProvider);
    this.prefix = requireNonNull(prefix);
  }

  /**
   * @throws NoSuchElementException   when the provided {@code key} doesn't have a corresponding config value
   * @throws IllegalArgumentException when property can't be converted to {@code type}
   * @throws IllegalStateException    when provider is unable to fetch configuration value for the given {@code key}
   */
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) {
    Type returnType = method.getGenericReturnType();
    return simpleConfigurationProvider.getProperty(prefix + (prefix.isEmpty() ? "" : ".") + method.getName(), () -> returnType);
  }
}