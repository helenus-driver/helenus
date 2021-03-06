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
package org.helenus.driver.junit.util;

/**
 * The <code>Strings</code> class provides a very simple pair of strings.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 28, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class Strings {
  /**
   * Holds the key.
   *
   * @author paouelle
   */
  public final String key;

  /**
   * Holds the value.
   *
   * @author paouelle
   */
  public final String value;

  /**
   * Instantiates a new <code>Strings</code> object.
   *
   * @author paouelle
   *
   * @param key the key
   * @param value the value
   */
  public Strings(String key, String value) {
    this.key = key;
    this.value = value;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return key + '=' + value;
  }
}