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
package org.helenus.driver.impl;

import org.helenus.driver.TableWith;

/**
 * The <code>TableWithImpl</code> class defines options to be used when
 * creating tables.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class TableWithImpl
  extends Utils.Appendeable
  implements TableWith {
  /**
   * Holds the name for this option.
   *
   * @author paouelle
   */
  private final String name;

  /**
   * Holds the value for the option.
   *
   * @author paouelle
   */
  private final Object value;

  /**
   * Instantiates a new <code>TableWithImpl</code> object.
   *
   * @author paouelle
   *
   * @param  name the name of the option
   * @param  value the option's value
   * @throws NullPointerException if <code>name</code> or <code>value</code>
   *         is <code>null</code>
   */
  public TableWithImpl(String name, Object value) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null name");
    org.apache.commons.lang3.Validate.notNull(value, "invalid null value");
    this.name = name;
    this.value = value;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, java.lang.StringBuilder)
   */
  @Override
  void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
    sb.append(name).append("=");
    Utils.appendValue(value, null, sb);
  }

  /**
   * Gets the name of the option.
   *
   * @author paouelle
   *
   * @return the name of the option
   */
  public String getName() {
    return name;
  }
}
