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
package org.helenus.driver.codecs;

import java.util.Map;
import java.util.function.Supplier;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * The <code>MandatoryMapCodec</code> class provides an implementation
 * for a codec capable of handling empty mandatory collections.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 10, 2017 - paouelle - Creation
 *
 * @param <T> the type of collection
 * @param <K> the type of keys for the map
 * @param <V> the type of values for the map
 *
 * @since 3.0
 */
public class MandatoryMapCodec<K, V, T extends Map<K, V>>
  extends TypeCodec<T> {
  /**
   * Holds the codec used to decode maps.
   *
   * @author paouelle
   */
  private final TypeCodec<T> icodec;

  /**
   * Holds the supplier to use for creating empty maps.
   *
   * @author paouelle
   */
  private final Supplier<T> supplier;

  /**
   * Instantiates a new <code>MandatoryCollectionCodec</code> object.
   *
   * @author paouelle
   *
   * @param icodec the codec to use to decode maps
   * @param supplier the supplier to use for creating empty maps
   */
  public MandatoryMapCodec(TypeCodec<T> icodec, Supplier<T> supplier) {
    super(icodec.getCqlType(), icodec.getJavaType());
    this.icodec = icodec;
    this.supplier = supplier;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#parse(java.lang.String)
   */
  @Override
  public T parse(String value) throws InvalidTypeException {
    if (StringUtils.isEmpty(value) || value.equalsIgnoreCase("NULL")) {
      return supplier.get();
    }
    final T t = icodec.parse(value);

    return (t != null) ? t : supplier.get();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#format(java.lang.Object)
   */
  @Override
  public String format(T value) throws InvalidTypeException {
    if (value == null) {
      return "{}";
    }
    final String s = icodec.format(value);

    return (s != null) ? s : "{}";
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#serialize(java.lang.Object, com.datastax.driver.core.ProtocolVersion)
   */
  @Override
  public ByteBuffer serialize(T value, ProtocolVersion protocolVersion)
    throws InvalidTypeException {
    if (value == null) {
      value = supplier.get();
    }
    return icodec.serialize(value, protocolVersion);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#deserialize(java.nio.ByteBuffer, com.datastax.driver.core.ProtocolVersion)
   */
  @Override
  public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
    throws InvalidTypeException {
    if ((bytes == null) || (bytes.remaining() == 0)) {
      return supplier.get();
    }
    final T t = icodec.deserialize(bytes, protocolVersion);

    return (t != null) ? t : supplier.get();
  }
}
