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
package com.github.helenusdriver.persistence;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.net.InetAddress;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * The <code>DataType</code> enumeration defines Cassandra data types
 * for columns when one cannot rely on the default behavior where the type is
 * inferred from the field type.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 *
 * @see com.datastax.driver.core.utils.UUIDs to help work with {@link #TIMEUUID}
 */
@SuppressWarnings("javadoc")
public enum DataType implements CQLDataType {
  INFERRED("?", 0, Object.class),
  ASCII("ascii", 0, String.class),
  BIGINT("bigint", 0, Long.class),
  BLOB("blob", 0, byte[].class),
  BOOLEAN("boolean", 0, Boolean.class),
  COUNTER("counter", 0, Long.class),
  DECIMAL("decimal", 0, BigDecimal.class),
  DOUBLE("double", 0, Double.class),
  FLOAT("float", 0, Float.class),
  INET("inet", 0, InetAddress.class),
  INT("int", 0, Integer.class),
  TEXT("text", 0, String.class),
  TIMESTAMP("timestamp", 0, Date.class),
  UUID("uuid", 0, UUID.class),
  VARCHAR("varchar", 0, String.class),
  VARINT("varint", 0, BigInteger.class),
  TIMEUUID("timeuuid", 0, UUID.class),
  LIST("list", 1, List.class),
  SET("set", 1, Set.class),
  MAP("map", 2, Map.class);

  /**
   * Checks if altering a column from a specified data type to a specified data
   * type is supported.
   * <p>
   * <i>Note:</i> For collections, the data type must remain the same and the
   * arguments must also be alterable.
   *
   * @author paouelle
   *
   * @param  from the current data type of the column to be changed
   * @param  to the data type to change the column to
   * @return <code>true</code> if the conversion is supported; <code>false</code>
   *         otherwise
   */
  private static boolean isAlterable(DataType from, DataType to) {
    if (from == to) {
      return true;
    }
    switch (from) {
      case ASCII:
        return (to == TEXT) || (to == VARCHAR);
      case BIGINT:
        return (to == TIMESTAMP);
      case TEXT:
        return (to == VARCHAR);
      case TIMESTAMP:
        return (to == BIGINT) || (to == VARINT);
      case VARCHAR:
        return (to == TEXT);
      case TIMEUUID:
        return (to == UUID);
      default:
        return false;
    }
  }

  /**
   * Holds the non-<code>null</code> CQL name for the data type.
   *
   * @author paouelle
   */
  public final String CQL;

  /**
   * Holds the number of arguments for this collection data type.
   *
   * @author paouelle
   */
  public final int NUM_ARGUMENTS;

  /**
   * Holds the corresponding Java class
   *
   * @author paouelle
   */
  public final Class<?> CLASS;

  /**
   * Instantiates a new <code>DataType</code> object.
   *
   * @author paouelle
   *
   * @param cql the non-<code>null</code> CQL name for the data type
   * @param num the number of arguments for this collection data type
   * @param clazz the non-<code>null</code> Java class corresponding to
   *        the data type
   */
  private DataType(String cql, int num, Class<?> clazz) {
    this.CQL = cql;
    this.NUM_ARGUMENTS = num;
    this.CLASS = clazz;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.persistence.CQLDataType#isCollection()
   */
  @Override
  public boolean isCollection() {
    return NUM_ARGUMENTS != 0;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.persistence.CQLDataType#isUserDefined()
   */
  @Override
  public boolean isUserDefined() { // this enum only represents system types
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.persistence.CQLDataType#isAlterableTo(com.github.helenusdriver.persistence.CQLDataType)
   */
  @Override
  public boolean isAlterableTo(CQLDataType to) {
    if (to instanceof DataType) {
      return DataType.isAlterable(this, (DataType)to);
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.persistence.CQLDataType#toCQL()
   */
  @Override
  public String toCQL() {
    return CQL;
  }
}
