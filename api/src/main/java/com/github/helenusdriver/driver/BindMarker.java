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
package com.github.helenusdriver.driver;

/**
 * The <code>BindMarker</code> class provides a CQL3 bind marker.
 * <p>
 * This can be either an anonymous bind marker or a named one (but note that
 * named ones are only supported starting in Cassandra 2.0.1).
 * <p>
 * Please note that to create a new bind maker object you should use
 * {@link StatementBuilder#bindMarker()} (anonymous marker) or
 * {@link StatementBuilder#bindMarker(String)} (named marker).
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Mar 31, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class BindMarker {
  /**
   * Holds an anonymous bind marker.
   *
   * @author paouelle
   */
  static final BindMarker ANONYMOUS = new BindMarker(null);

  /**
   * Holds the name of this bind marker.
   *
   * @author paouelle
   */
  private final String name;

  /**
   * Instantiates a new <code>BindMarker</code> object.
   *
   * @author paouelle
   *
   * @param name the name of this bind marker
   */
  BindMarker(String name) {
    this.name = name;
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
    if (name == null) {
      return "?";
    }
    return StatementManager.getManager().appendName(
      name, new StringBuilder(name.length() + 1).append(':')
    ).toString();
  }
}
