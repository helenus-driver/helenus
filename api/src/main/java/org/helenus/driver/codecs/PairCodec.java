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

import java.nio.ByteBuffer;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.TypeCodec.AbstractTupleCodec;
import com.google.common.reflect.TypeToken;

/**
 * The <code>PairCodec</code> class provides an implementation for a codec
 * capable of handling {@link Pair} objects.
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
public class PairCodec<L, R> extends AbstractTupleCodec<Pair<L, R>> {
  /**
   * Holds the codec to decode the left side.
   *
   * @author paouelle
   */
  private final TypeCodec<L> lcodec;

  /**
   * Holds the codec to decode the right side.
   *
   * @author paouelle
   */
  private final TypeCodec<R> rcodec;

  /**
   * Instantiates a new <code>PairCodec</code> object.
   *
   * @author paouelle
   *
   * @param cqlType the CQL type for this codec
   * @param javaType the java type for this codec
   * @param lcodec the left codec
   * @param rcodec the right codec
   */
  public PairCodec(
    TupleType cqlType,
    TypeToken<Pair<L, R>> javaType,
    TypeCodec<L> lcodec,
    TypeCodec<R> rcodec
  ) {
    super(cqlType, javaType);
    this.lcodec = lcodec;
    this.rcodec = rcodec;
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
    if (!super.accepts(value)) {
      return false;
    }
    if (value instanceof Pair) { // runtime type ok, now check each sides
      final Pair<?, ?> p = (Pair<?, ?>)value;

      return lcodec.accepts(p.getLeft()) && rcodec.accepts(p.getRight());
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec.AbstractTupleCodec#newInstance()
   */
  @Override
  protected Pair<L, R> newInstance() {
    return new MutablePair<>();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec.AbstractTupleCodec#formatField(java.lang.Object, int)
   */
  @Override
  protected String formatField(Pair<L, R> source, int index) {
    if (index == 0) {
      return lcodec.format(source.getLeft());
    } else if (index == 1) {
      return rcodec.format(source.getRight());
    }
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec.AbstractTupleCodec#parseAndSetField(java.lang.String, java.lang.Object, int)
   */
  @Override
  protected Pair<L, R> parseAndSetField(String input, Pair<L, R> target, int index) {
    if (index == 0) {
      ((MutablePair<L, R>)target).setLeft(lcodec.parse(input));
    } else if (index == 1) {
      ((MutablePair<L, R>)target).setRight(rcodec.parse(input));
    }
    return target;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec.AbstractTupleCodec#serializeField(java.lang.Object, int, com.datastax.driver.core.ProtocolVersion)
   */
  @Override
  protected ByteBuffer serializeField(Pair<L, R> source, int index, ProtocolVersion protocolVersion) {
    if (index == 0) {
      return lcodec.serialize(source.getLeft(), protocolVersion);
    } else if (index == 1) {
      return rcodec.serialize(source.getRight(), protocolVersion);
    }
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec.AbstractTupleCodec#deserializeAndSetField(java.nio.ByteBuffer, java.lang.Object, int, com.datastax.driver.core.ProtocolVersion)
   */
  @Override
  protected Pair<L, R> deserializeAndSetField(ByteBuffer input, Pair<L, R> target, int index, ProtocolVersion protocolVersion) {
    if (index == 0) {
      ((MutablePair<L, R>)target).setLeft(lcodec.deserialize(input, protocolVersion));
    } else if (index == 1) {
      ((MutablePair<L, R>)target).setRight(rcodec.deserialize(input, protocolVersion));
    }
    return target;
  }
}
