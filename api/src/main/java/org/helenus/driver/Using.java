/*
 * Copyright (C) 2015-2015 The Helenus Driver Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.helenus.driver;

/**
 * The <code>Using</code> class extends Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Using} to provide support for
 * POJOs.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2016 - paouelle - Creation
 *
 * @param <T> the type of the option
 *
 * @since 1.0
 */
public interface Using<T> {
  /**
   * Holds the TTL option name.
   *
   * @author paouelle
   */
  public final static String TTL = "TTL";

  /**
   * Holds the TIMESTAMP option name.
   *
   * @author paouelle
   */
  public final static String TIMESTAMP = "TIMESTAMP";

  /**
   * Gets the option's name.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> option's name
   */
  public String getName();

  /**
   * Gets the option's value.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> option's value
   */
  public T getValue();

  /**
   * Sets the value for this option.
   *
   * @author paouelle
   *
   * @param  value the non-<code>null</code> new option's value
   * @throws NullPointerException if <code>value</code> is <code>null</code>
   */
  public void setValue(T value);
}
