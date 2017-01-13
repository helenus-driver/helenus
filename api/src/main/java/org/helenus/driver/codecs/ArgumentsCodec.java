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

import java.util.Arrays;
import java.util.List;

import java.nio.ByteBuffer;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * The <code>ArgumentsCodec</code> class wraps around another codec in order to
 * provide access to the data type's arguments; used for lists, sets, maps, and
 * tuples.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 10, 2017 - paouelle - Creation
 *
 * @param <T> the type for this codec
 *
 * @since 3.0
 */
public class ArgumentsCodec<T> extends TypeCodec<T> {
  /**
   * Holds the codec used to decode collections.
   *
   * @author paouelle
   */
  private final TypeCodec<T> icodec;

  /**
   * Holds the codecs used to decode arguments.
   *
   * @author paouelle
   */
  private final List<TypeCodec<?>> acodecs;

  /**
   * Instantiates a new <code>ArgumentsCodec</code> object.
   *
   * @author paouelle
   *
   * @param icodec the codec to use to decode the type
   * @param acodecs the codes for all arguments
   */
  public ArgumentsCodec(TypeCodec<T> icodec, TypeCodec<?>... acodecs) {
    super(icodec.getCqlType(), icodec.getJavaType());
    this.icodec = icodec;
    this.acodecs = Arrays.asList(acodecs);
  }

  /**
   * Gets the i-th argument codec.
   *
   * @author paouelle
   *
   * @param  i the index of the argument to get the codec for
   * @return the corresponding non-<code>null</code> codec
   * @throws IndexOutOfBoundsException if the specified index is out of bounds
   */
  public TypeCodec<?> codec(int i) {
    return acodecs.get(i);
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
    return icodec.parse(value);
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
  public ByteBuffer serialize(T value, ProtocolVersion protocolVersion)
    throws InvalidTypeException {
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
    return icodec.deserialize(bytes, protocolVersion);
  }
}
