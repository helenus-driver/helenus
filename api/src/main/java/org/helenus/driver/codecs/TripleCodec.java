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

import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.TypeCodec.AbstractTupleCodec;
import com.google.common.reflect.TypeToken;

/**
 * The <code>TripleCodec</code> class provides an implementation for a codec
 * capable of handling {@link Triple} objects.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 13, 2017 - paouelle - Creation
 *
 * @param <L> the left element type
 * @param <M> the middle element type
 * @param <R> the right element type
 *
 * @since 3.0
 */
public class TripleCodec<L, M, R> extends AbstractTupleCodec<Triple<L, M, R>> {
  /**
   * Holds the codec to decode the left side.
   *
   * @author paouelle
   */
  private final TypeCodec<L> lcodec;

  /**
   * Holds the codec to decode the middle side.
   *
   * @author paouelle
   */
  private final TypeCodec<M> mcodec;

  /**
   * Holds the codec to decode the right side.
   *
   * @author paouelle
   */
  private final TypeCodec<R> rcodec;

  /**
   * Instantiates a new <code>TripleCodec</code> object.
   *
   * @author paouelle
   *
   * @param cqlType the CQL type for this codec
   * @param javaType the java type for this codec
   * @param lcodec the left codec
   * @param mcodec the middle codec
   * @param rcodec the right codec
   */
  public TripleCodec(
    TupleType cqlType,
    TypeToken<Triple<L, M, R>> javaType,
    TypeCodec<L> lcodec,
    TypeCodec<M> mcodec,
    TypeCodec<R> rcodec
  ) {
    super(cqlType, javaType);
    this.lcodec = lcodec;
    this.mcodec = mcodec;
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
    if (value instanceof Triple) { // runtime type ok, now check each sides
      final Triple<?, ?, ?> p = (Triple<?, ?, ?>)value;

      return (
        lcodec.accepts(p.getLeft())
        && mcodec.accepts(p.getMiddle())
        && rcodec.accepts(p.getRight())
      );
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
  protected Triple<L, M, R> newInstance() {
    return new MutableTriple<>();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec.AbstractTupleCodec#formatField(java.lang.Object, int)
   */
  @Override
  protected String formatField(Triple<L, M, R> source, int index) {
    if (index == 0) {
      return lcodec.format(source.getLeft());
    } else if (index == 1) {
      return mcodec.format(source.getMiddle());
    } else if (index == 2) {
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
  protected Triple<L, M, R> parseAndSetField(String input, Triple<L, M, R> target, int index) {
    if (index == 0) {
      ((MutableTriple<L, M, R>)target).setLeft(lcodec.parse(input));
    } else if (index == 1) {
      ((MutableTriple<L, M, R>)target).setMiddle(mcodec.parse(input));
    } else if (index == 2) {
      ((MutableTriple<L, M, R>)target).setRight(rcodec.parse(input));
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
  protected ByteBuffer serializeField(Triple<L, M, R> source, int index, ProtocolVersion protocolVersion) {
    if (index == 0) {
      return lcodec.serialize(source.getLeft(), protocolVersion);
    } else if (index == 1) {
      return mcodec.serialize(source.getMiddle(), protocolVersion);
    } else if (index == 2) {
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
  protected Triple<L, M, R> deserializeAndSetField(ByteBuffer input, Triple<L, M, R> target, int index, ProtocolVersion protocolVersion) {
    if (index == 0) {
      ((MutableTriple<L, M, R>)target).setLeft(lcodec.deserialize(input, protocolVersion));
    } else if (index == 1) {
      ((MutableTriple<L, M, R>)target).setMiddle(mcodec.deserialize(input, protocolVersion));
    } else if (index == 2) {
      ((MutableTriple<L, M, R>)target).setRight(rcodec.deserialize(input, protocolVersion));
    }
    return target;
  }
}
