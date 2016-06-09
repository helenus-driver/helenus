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

import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import org.helenus.driver.persistence.CQLDataType;

/**
 * The <code>UDTValueWrapper</code> class is used to track a user-defined POJO
 * such that it be ready for encoding.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Mar 6, 2015 - paouelle - Creation
 *
 * @param <T> the decoded type of the elements
 *
 * @since 1.0
 */
public class UDTValueWrapper<T> implements PersistedObject<T, T> {
  /**
   * Holds the class info for the user-defined type.
   *
   * @author paouelle
   */
  private final UDTClassInfoImpl<T> cinfo;

  /**
   * Holds the user-defined value.
   *
   * @author paouelle
   */
  private final T val;

  /**
   * Instantiates a new <code>PersistedUDTValue</code> object.
   *
   * @author paouelle
   *
   * @param  cinfo the class info for the user-defined value
   * @param  val the user-defined value
   * @throws NullPointerException if <code>val</code> is <code>null</code>
   * @throws ClassCastException if the value is not of the proper class
   */
  UDTValueWrapper(UDTClassInfoImpl<T> cinfo, Object val) {
    org.apache.commons.lang3.Validate.notNull(val, "invalid null value");
    this.cinfo = cinfo;
    this.val = this.cinfo.getObjectClass().cast(val);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(val);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof UDTValueWrapper) {
      @SuppressWarnings("unchecked")
      final UDTValueWrapper<T> pval = (UDTValueWrapper<T>)obj;

      return (cinfo == pval.cinfo) && Objects.equals(val, pval.val);
    }
    return false;
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
    final Map<String, Pair<Object, CQLDataType>> columns = cinfo.newContext(val).getColumnValues();
    final StringBuilder sb = new StringBuilder();

    sb.append("{");
    Utils.joinAndAppendNamesAndValues(sb, ",", ":", columns).append("}");
    return sb.toString();
  }
}
