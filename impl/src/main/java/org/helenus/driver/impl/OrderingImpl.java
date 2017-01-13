/*
 * Copyright (C) 2015-2017 The Helenus Driver Project Authors.
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

import java.util.List;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;

import org.helenus.driver.Ordering;

/**
 * The <code>OrderingImpl</code> class extends Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Ordering} to provide support for
 * POJOs.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class OrderingImpl
  extends Utils.Appendeable
  implements Ordering {
  /**
   * Holds the name of the column being ordered.
   *
   * @author paouelle
   */
  private final CharSequence name;

  /**
   * <code>true</code> if the order is descending; <code>false</code> otherwise.
   *
   * @author paouelle
   */
  private final boolean isDesc;

  /**
   * Instantiates a new <code>OrderingImpl</code> object.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  isDesc <code>true</code> if the order is descending; <code>false</code>
   *         otherwise
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  OrderingImpl(CharSequence name, boolean isDesc) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    this.name = name;
    this.isDesc = isDesc;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
   */
  @Override
  void appendTo(
    TableInfoImpl<?> tinfo,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    List<Object> variables
  ) {
    Utils.appendName(tinfo, null, codecRegistry, sb, name);
    sb.append(isDesc ? " DESC" : " ASC");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
   */
  @Override
  boolean containsBindMarker() {
    return false;
  }

  /**
   * Gets the column name for this ordering.
   *
   * @author paouelle
   *
   * @return the column name for this ordering
   */
  CharSequence getColumnName() {
    return name;
  }

  /**
   * Checks if the order is descending.
   *
   * @author paouelle
   *
   * @return <code>true</code> if the order is descending; <code>false</code>
   *         otherwise
   */
  boolean isDescending() {
    return isDesc;
  }

  /**
   * Validates the clause using the specified table.
   *
   * @author paouelle
   *
   * @param <T> the type of POJO
   *
   * @param  table the non-<code>null</code> table to validate the ordering with
   * @throws IllegalArgumentException if the ordering is not valid
   */
  <T> void validate(TableInfoImpl<T> table) {
    table.validateColumn(name);
  }
}
