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
package org.helenus.driver.persistence;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.net.InetAddress;

import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

import javax.json.JsonStructure;

import org.apache.commons.lang3.ClassUtils;

import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.google.common.reflect.TypeToken;

import org.helenus.driver.codecs.provider.AsciiCodecProvider;
import org.helenus.driver.codecs.provider.BigIntCodecProvider;
import org.helenus.driver.codecs.provider.BlobCodecProvider;
import org.helenus.driver.codecs.provider.BooleanCodecProvider;
import org.helenus.driver.codecs.provider.CodecProvider;
import org.helenus.driver.codecs.provider.CounterCodecProvider;
import org.helenus.driver.codecs.provider.DateCodecProvider;
import org.helenus.driver.codecs.provider.DecimalCodecProvider;
import org.helenus.driver.codecs.provider.DoubleCodecProvider;
import org.helenus.driver.codecs.provider.FloatCodecProvider;
import org.helenus.driver.codecs.provider.InetCodecProvider;
import org.helenus.driver.codecs.provider.IntCodecProvider;
import org.helenus.driver.codecs.provider.SmallIntCodecProvider;
import org.helenus.driver.codecs.provider.TextCodecProvider;
import org.helenus.driver.codecs.provider.TimeCodecProvider;
import org.helenus.driver.codecs.provider.TimeUUIDCodecProvider;
import org.helenus.driver.codecs.provider.TimestampCodecProvider;
import org.helenus.driver.codecs.provider.TinyIntCodecProvider;
import org.helenus.driver.codecs.provider.UUIDCodecProvider;
import org.helenus.driver.codecs.provider.VarCharCodecProvider;
import org.helenus.driver.codecs.provider.VarIntCodecProvider;

/**
 * The <code>DataType</code> enumeration defines Cassandra data types
 * for columns when one cannot rely on the default behavior where the type is
 * inferred from the field type.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
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
  INFERRED(Name.CUSTOM, "?", 0),

  ASCII(Name.ASCII, "ascii", 0, AsciiCodecProvider.INSTANCE),
  BIGINT(Name.BIGINT, "bigint", 0, BigIntCodecProvider.INSTANCE),
  BLOB(Name.BLOB, "blob", 0, BlobCodecProvider.INSTANCE),
  BOOLEAN(Name.BOOLEAN, "boolean", 0, BooleanCodecProvider.INSTANCE),
  COUNTER(Name.COUNTER, "counter", 0, CounterCodecProvider.INSTANCE),
  DECIMAL(Name.DECIMAL, "decimal", 0, DecimalCodecProvider.INSTANCE),
  DOUBLE(Name.DOUBLE, "double", 0, DoubleCodecProvider.INSTANCE),
  FLOAT(Name.FLOAT, "float", 0, FloatCodecProvider.INSTANCE),
  INET(Name.INET, "inet", 0, InetCodecProvider.INSTANCE),
  INT(Name.INT, "int", 0, IntCodecProvider.INSTANCE),
  TEXT(Name.TEXT, "text", 0, TextCodecProvider.INSTANCE),
  TIMESTAMP(Name.TIMESTAMP, "timestamp", 0, TimestampCodecProvider.INSTANCE),
  UUID(Name.UUID, "uuid", 0, UUIDCodecProvider.INSTANCE),
  VARCHAR(Name.VARCHAR, "varchar", 0, VarCharCodecProvider.INSTANCE),
  VARINT(Name.VARINT, "varint", 0, VarIntCodecProvider.INSTANCE),
  TIMEUUID(Name.TIMEUUID, "timeuuid", 0, TimeUUIDCodecProvider.INSTANCE),
  SMALLINT(Name.SMALLINT, "smallint", 0, SmallIntCodecProvider.INSTANCE),
  TINYINT(Name.TINYINT, "tinyint", 0, TinyIntCodecProvider.INSTANCE),
  DATE(Name.DATE, "date", 0, DateCodecProvider.INSTANCE),
  TIME(Name.TIME, "time", 0, TimeCodecProvider.INSTANCE),

  TUPLE(Name.TUPLE, "tuple", -1),

  LIST(Name.LIST, "list", 1),
  SET(Name.SET, "set", 1),
  ORDERED_SET(Name.LIST, "list", 1),
  SORTED_SET(Name.SET, "set", 1),
  MAP(Name.MAP, "map", 2),
  SORTED_MAP(Name.MAP, "map", 2);

  /**
   * Gets a suitable default data type for the specified class.
   *
   * @author paouelle
   *
   * @param  clazz the class for which to get a default data type
   * @return the corresponding default data type or <code>null</code> if no
   *         default data type is defined for the specified class
   */
  public static DataType valueOf(Class<?> clazz) {
    if (clazz == null) {
      return null;
    }
    clazz = ClassUtils.primitiveToWrapper(clazz);
    if (String.class == clazz) {
      return DataType.TEXT;
    } else if (Boolean.class == clazz) {
      return DataType.BOOLEAN;
    } else if (Integer.class == clazz) {
      return DataType.INT;
    } else if (Long.class == clazz) {
      return DataType.BIGINT;
    } else if (Short.class == clazz) {
      return DataType.SMALLINT;
    } else if (Byte.class == clazz) {
      return DataType.TINYINT;
    } else if (Double.class == clazz) {
      return DataType.DOUBLE;
    } else if (Float.class == clazz) {
      return DataType.FLOAT;
    } else if (UUID.class.isAssignableFrom(clazz)) {
      return DataType.UUID;
    } else if ((clazz.isArray() && (Byte.TYPE == clazz.getComponentType()))
               || ByteBuffer.class.isAssignableFrom(clazz)) {
      return DataType.BLOB;
    } else if (clazz.isEnum()) {
      return DataType.ASCII;
    } else if (Class.class == clazz) {
      return DataType.ASCII;
    } else if (Locale.class == clazz) {
      return DataType.ASCII;
    } else if (ZoneId.class.isAssignableFrom(clazz)) {
      return DataType.ASCII;
    } else if (Date.class.isAssignableFrom(clazz)
               || Instant.class.isAssignableFrom(clazz)) {
      return DataType.TIMESTAMP;
    } else if (BigDecimal.class.isAssignableFrom(clazz)) {
      return DataType.DECIMAL;
    } else if (BigInteger.class.isAssignableFrom(clazz)) {
      return DataType.VARINT;
    } else if (AtomicLong.class.isAssignableFrom(clazz)) {
      return DataType.COUNTER;
    } else if (Instant.class == clazz) {
      return DataType.TIMESTAMP;
    } else if (AtomicInteger.class.isAssignableFrom(clazz)) {
      return DataType.COUNTER;
    } else if (LocalDate.class.isAssignableFrom(clazz)
               || com.datastax.driver.core.LocalDate.class.isAssignableFrom(clazz)) {
      return DataType.DATE;
    } else if (LocalTime.class.isAssignableFrom(clazz)) {
      return DataType.TIME;
    } else if (InetAddress.class.isAssignableFrom(clazz)) {
      return DataType.INET;
    } else if (JsonStructure.class.isAssignableFrom(clazz)) {
      return DataType.VARCHAR;
    }
    return null;
  }

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
        return (to == TIMESTAMP) || (to == TIME);
      case INT:
        return (to == DATE);
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
   * Gets a data type for a given data type name.
   *
   * @author paouelle
   *
   * @param  name the data type for which to get its corresponding data type
   * @return the corresponding non-<code>null</code> data type
   * @throws IllegalArgumentException if no data type corresponds to the provided name
   */
  public static DataType valueOf(Name name) {
    for (final DataType type: DataType.values()) {
      if (type.NAME.equals(name)) {
        return type;
      }
    }
    throw new IllegalArgumentException("unknown data type: " + name);
  }

  /**
   * Holds the non-<code>null</code> Cassandra name for the data type.
   *
   * @author paouelle
   */
  public final Name NAME;

  /**
   * Holds the corresponding Cassandra primitive data type if this is a primitive
   * data type; <code>null</code> otherwise.
   *
   * @author paouelle
   */
  public final com.datastax.driver.core.DataType TYPE;

  /**
   * Holds the non-<code>null</code> CQL name for the data type.
   *
   * @author paouelle
   */
  public final String CQL;

  /**
   * Holds the number of arguments for this collection data type.
   * <p>
   * <i>Note:</i> Will be <code>-1</code> if supports a variable number of
   * arguments (at least one).
   *
   * @author paouelle
   */
  public final int NUM_ARGUMENTS;

  /**
   * Holds the codec provider that can be used to decode this data type. May
   * be <code>null</code>.
   *
   * @author paouelle
   */
  private final CodecProvider provider;

  /**
   * Instantiates a new <code>DataType</code> object.
   *
   * @author paouelle
   *
   * @param name the non-<code>null</code> Cassandra name for the data type
   * @param cql the non-<code>null</code> CQL name for the data type
   * @param num the number of arguments for this collection data type
   * @param a set of codec providers for this data type
   */
  private DataType(
    Name name,
    String cql,
    int num
  ) {
    this(name, cql, num, null);
  }

  /**
   * Instantiates a new <code>DataType</code> object.
   *
   * @author paouelle
   *
   * @param name the non-<code>null</code> Cassandra name for the data type
   * @param cql the non-<code>null</code> CQL name for the data type
   * @param num the number of arguments for this collection data type (<code>-1</code>
   *        for a variable number of arguments greater than <code>0</code>)
   * @param provider a codec provider for this data type (may be <code>null</code>)
   */
  private DataType(
    Name name,
    String cql,
    int num,
    CodecProvider provider
  ) {
    this.NAME = name;
    this.TYPE = com.datastax.driver.core.DataType.allPrimitiveTypes().stream()
      .filter(dt -> name.equals(dt.getName()))
      .findFirst()
      .orElse(null);
    this.CQL = cql;
    this.NUM_ARGUMENTS = num;
    this.provider = provider;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.CQLDataType#isFrozen()
   */
  @Override
  public boolean isFrozen() {
    return isTuple();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.CQLDataType#isCollection()
   */
  @Override
  public boolean isCollection() {
    return (NUM_ARGUMENTS != 0) && !isTuple();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.CQLDataType#isTuple()
   */
  @Override
  public boolean isTuple() {
    return (this == DataType.TUPLE);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.CQLDataType#isUserDefined()
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
   * @throws IllegalArgumentException if this data type is not a primitive one
   *
   * @see org.helenus.driver.persistence.CQLDataType#getDataType()
   */
  @Override
  public com.datastax.driver.core.DataType getDataType() {
    org.apache.commons.lang3.Validate.isTrue(
      TYPE != null, "not a primitive data type: " + name()
    );
    return TYPE;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.CQLDataType#getMainType()
   */
  @Override
  public CQLDataType getMainType() {
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.CQLDataType#getElementType()
   */
  @Override
  public CQLDataType getElementType() {
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.CQLDataType#getArgumentTypes()
   */
  @Override
  public List<CQLDataType> getArgumentTypes() {
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.CQLDataType#getFirstArgumentType()
   */
  @Override
  public CQLDataType getFirstArgumentType() {
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.CQLDataType#isAlterableTo(org.helenus.driver.persistence.CQLDataType)
   */
  @Override
  public boolean isAlterableTo(CQLDataType to) {
    if (to instanceof DataType) {
      return DataType.isAlterable(this, (DataType)to);
    }
    return false;
  }

  /**
   * Gets a {@link TypeCodec codec} that accepts the given class from this data
   * type.
   *
   * @author paouelle
   *
   * @param <T> the type of objects to decode
   *
   * @param  clazz the class the codec should accept
   * @return a suitable codec
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws CodecNotFoundException if a suitable codec cannot be found
   */
  public <T> TypeCodec<T> codecFor(Class<T> clazz) throws CodecNotFoundException {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    if (provider == null) {
      throw new CodecNotFoundException(
        "unsupported codec from '"
        + toCQL()
        + "' to class: "
        + clazz.getName(),
        getDataType(),
        TypeToken.of(clazz)
      );
    }
    return provider.codecFor(clazz);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.CQLDataType#toCQL()
   */
  @Override
  public String toCQL() {
    return CQL;
  }
}
