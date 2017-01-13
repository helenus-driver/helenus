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

import java.util.function.Supplier;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * The <code>MandatoryPairCodec</code> class provides an implementation
 * for a codec capable of handling empty mandatory pairs.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 13, 2017 - paouelle - Creation
 *
 * @param <L> the left element type
 * @param <R> the right element type
 *
 * @since 3.0
 */
public class MandatoryPairCodec<L, R> extends TypeCodec<Pair<L, R>> {
  /**
   * Holds the codec used to decode pairs.
   *
   * @author paouelle
   */
  private final TypeCodec<Pair<L, R>> icodec;

  /**
   * Holds the supplier to use for creating empty pairs.
   *
   * @author paouelle
   */
  private final Supplier<Pair<L, R>> supplier;

  /**
   * Instantiates a new <code>MandatoryPairCodec</code> object.
   *
   * @author paouelle
   *
   * @param icodec the codec to use to decode pairs
   * @param supplier the supplier to use for creating empty pairs
   */
  public MandatoryPairCodec(TypeCodec<Pair<L, R>> icodec, Supplier<Pair<L, R>> supplier) {
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
  public Pair<L, R> parse(String value) throws InvalidTypeException {
    if (StringUtils.isEmpty(value) || value.equalsIgnoreCase("NULL")) {
      return supplier.get();
    }
    final Pair<L, R> t = icodec.parse(value);

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
  public String format(Pair<L, R> value) throws InvalidTypeException {
    if (value == null) {
      value = supplier.get();
    }
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
  public ByteBuffer serialize(Pair<L, R> value, ProtocolVersion protocolVersion)
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
  public Pair<L, R> deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
    throws InvalidTypeException {
    if ((bytes == null) || (bytes.remaining() == 0)) {
      return supplier.get();
    }
    final Pair<L, R> t = icodec.deserialize(bytes, protocolVersion);

    return (t != null) ? t : supplier.get();
  }
}
