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
package org.helenus.driver.codecs.provider;

import java.util.concurrent.atomic.AtomicLong;

import java.nio.ByteBuffer;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.google.common.reflect.TypeToken;

import org.helenus.driver.persistence.DataType;

/**
 * The <code>CounterCodecProvider</code> class defines a provider suitable to
 * decode the {@link DataType#COUNTER} data type.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 9, 2017 - paouelle - Creation
 *
 * @since 1.0
 */
public final class CounterCodecProvider implements CodecProvider {
  /**
   * Holds the single instance of this provider.
   *
   * @author paouelle
   */
  public final static CodecProvider INSTANCE = new CounterCodecProvider();

  /**
   * Gets a codec to decode {@link AtomicLong} objects.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> codec to decode {@link AtomicLong} objects
   */
  @SuppressWarnings("synthetic-access")
  public static TypeCodec<AtomicLong> atomicLong() {
    return AtomicLongCodec.instance;
  }

  /**
   * Instantiates a new <code>CounterCodecProvider</code> object.
   *
   * @author paouelle
   */
  private CounterCodecProvider() {}

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.codecs.provider.CodecProvider#codecFor(java.lang.Class)
   */
  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public <T> TypeCodec<T> codecFor(Class<T> clazz)
    throws CodecNotFoundException {
    if ((Long.class == clazz) || (Long.TYPE == clazz)) {
      return (TypeCodec<T>)TypeCodec.counter();
    } else if (AtomicLong.class.isAssignableFrom(clazz)) {
      return (TypeCodec<T>)CounterCodecProvider.atomicLong();
    }
    throw new CodecNotFoundException(
      "unsupported Helenus codec from '"
      + DataType.COUNTER.toCQL()
      + "' to class: "
      + clazz.getName(),
      com.datastax.driver.core.DataType.counter(),
      TypeToken.of(clazz)
    );
  }

  /**
   * The <code>AtomicLongCodec</code> class defines a type codec for {@link AtomicLong}
   * objects.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author The Helenus Driver Project Authors
   * @version 1 - Jan 9, 2017 - paouelle - Creation
   *
   * @since 3.0
   */
  private static class AtomicLongCodec extends TypeCodec<AtomicLong> {
    /**
     * Holds the instance for this codec.
     *
     * @author paouelle
     */
    private static AtomicLongCodec instance = new AtomicLongCodec();

    /**
     * Holds the internal codec used to decode.
     *
     * @author paouelle
     */
    private static PrimitiveLongCodec icodec = TypeCodec.counter();

    /**
     * Instantiates a new <code>AtomicLongCodec</code> object.
     *
     * @author paouelle
     */
    private AtomicLongCodec() {
      super(com.datastax.driver.core.DataType.counter(), AtomicLong.class);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#parse(java.lang.String)
     */
    @Override
    public AtomicLong parse(String value) {
      final Long l = AtomicLongCodec.icodec.parse(value);

      return (l != null) ? new AtomicLong(l) : null;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#format(java.lang.Object)
     */
    @Override
    public String format(AtomicLong value) {
      return AtomicLongCodec.icodec.format((value != null) ? value.longValue() : null);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#serialize(java.lang.Object, com.datastax.driver.core.ProtocolVersion)
     */
    @Override
    public ByteBuffer serialize(AtomicLong value, ProtocolVersion protocolVersion) {
      return AtomicLongCodec.icodec.serialize((value != null) ? value.longValue() : null, protocolVersion);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#deserialize(java.nio.ByteBuffer, com.datastax.driver.core.ProtocolVersion)
     */
    @Override
    public AtomicLong deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
      final Long l = AtomicLongCodec.icodec.deserialize(bytes, protocolVersion);

      return (l != null) ? new AtomicLong(l) : null;
    }
  }
}