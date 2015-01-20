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
package com.github.helenus.driver.impl;

import com.github.helenus.driver.Using;

/**
 * The <code>UsingImpl</code> class extends Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Using} to provide support for
 * POJOs.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class UsingImpl
  extends Utils.Appendeable
  implements Using {
  /**
   * Holds the option name.
   *
   * @author paouelle
   */
  private final String optionName;

  /**
   * Holds the option's value.
   *
   * @author paouelle
   */
  private final long value;

  /**
   * Instantiates a new <code>UsingImpl</code> object.
   *
   * @author paouelle
   *
   * @param optionName the option name
   * @param value the option's value
   */
  UsingImpl(String optionName, long value) {
    this.optionName = optionName;
    this.value = value;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenus.driver.impl.Utils.Appendeable#appendTo(com.github.helenus.driver.impl.TableInfoImpl, java.lang.StringBuilder)
   */
  @Override
  void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
    sb.append(optionName).append(" ").append(value);
  }
}
