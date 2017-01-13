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
import java.util.SortedMap;
import java.util.TreeMap;

import java.nio.ByteBuffer;

import com.datastax.driver.core.DataType.CollectionType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.google.common.reflect.TypeToken;

/**
 * The <code>SortedMapCodec</code> class provides an implementation for a codec
 * capable of handling {@link SortedMap} objects.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 10, 2017 - paouelle - Creation
 *
 * @param <K> the type of keys for the map
 * @param <V> the type of values for the map
 *
 * @since 3.0
 */
public class SortedMapCodec<K, V> extends TypeCodec<SortedMap<K, V>> {
  /**
   * Holds the internal codec to deal with maps at its core.
   *
   * @author paouelle
   */
  private final TypeCodec<Map<K, V>> icodec;

  /**
   * Instantiates a new <code>SortedMapCodec</code> object.
   *
   * @author paouelle
   *
   * @param cqlType the CQL type for this codec
   * @param javaType the java type for this codec
   * @param kcodec the key codec
   * @param vcodec the value codec
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public SortedMapCodec(
    CollectionType cqlType,
    TypeToken<SortedMap<K, V>> javaType,
    TypeCodec<K> kcodec,
    TypeCodec<V> vcodec
  ) {
    super(cqlType, javaType);
    this.icodec = new AbstractMapCodec(kcodec, vcodec) {
      @Override
      protected Map<K, V> newInstance(int size) {
        return new TreeMap<>();
      }
    };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#accepts(java.lang.Object)
   */
  @Override
  public boolean accepts(Object value) {
    return (value instanceof SortedMap) && icodec.accepts(value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#parse(java.lang.String)
   */
  @Override
  public SortedMap<K, V> parse(String value) {
    return (SortedMap<K, V>)icodec.parse(value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#format(java.lang.Object)
   */
  @Override
  public String format(SortedMap<K, V> value) {
    return icodec.format(value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#serialize(java.lang.Object, com.datastax.driver.core.ProtocolVersion)
   */
  @Override
  public ByteBuffer serialize(SortedMap<K, V> value, ProtocolVersion protocolVersion) {
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
  public SortedMap<K, V> deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    return (SortedMap<K, V>)icodec.deserialize(bytes, protocolVersion);
  }
}
