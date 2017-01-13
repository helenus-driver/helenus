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

import java.nio.ByteBuffer;
import java.time.LocalDate;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.google.common.reflect.TypeToken;

import org.helenus.driver.persistence.DataType;

/**
 * The <code>DateCodecProvider</code> class defines a provider suitable to
 * decode the {@link DataType#DATE} data type.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 9, 2017 - paouelle - Creation
 *
 * @since 3.0
 */
public final class DateCodecProvider implements CodecProvider {
  /**
   * Holds the single instance of this provider.
   *
   * @author paouelle
   */
  public final static CodecProvider INSTANCE = new DateCodecProvider();

  /**
   * Gets a codec to decode {@link Integer} objects.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> codec to decode {@link Integer} objects
   */
  @SuppressWarnings("synthetic-access")
  public static TypeCodec<Integer> cint() {
    return IntegerCodec.instance;
  }

  /**
   * Instantiates a new <code>DateCodecProvider</code> object.
   *
   * @author paouelle
   */
  private DateCodecProvider() {}

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
    if ((Integer.class == clazz) || (Integer.TYPE == clazz)) {
      return (TypeCodec<T>)DateCodecProvider.cint();
    } else if (LocalDate.class.isAssignableFrom(clazz)) {
      return (TypeCodec<T>)LocalDateCodec.instance;
    } else if (com.datastax.driver.core.LocalDate.class.isAssignableFrom(clazz)) {
      return (TypeCodec<T>)TypeCodec.date();
    }
    throw new CodecNotFoundException(
      "unsupported Helenus codec from '"
      + DataType.DATE.toCQL()
      + "' to class: "
      + clazz.getName(),
      com.datastax.driver.core.DataType.date(),
      TypeToken.of(clazz)
    );
  }

  /**
   * The <code>IntegerCodec</code> class defines a type codec for {@link Integer}
   * objects.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author The Helenus Driver Project Authors
   * @version 1 - Jan 9, 2017 - paouelle - Creation
   *
   * @since 3.0
   */
  private static class IntegerCodec extends TypeCodec<Integer> {
    /**
     * Holds the instance for this codec.
     *
     * @author paouelle
     */
    private static IntegerCodec instance = new IntegerCodec();

    /**
     * Holds the internal codec used to decode.
     *
     * @author paouelle
     */
    private static TypeCodec<LocalDate> icodec = LocalDateCodec.instance;

    /**
     * Instantiates a new <code>IntegerCodec</code> object.
     *
     * @author paouelle
     */
    private IntegerCodec() {
      super(com.datastax.driver.core.DataType.date(), Integer.class);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#parse(java.lang.String)
     */
    @Override
    public Integer parse(String value) {
      final LocalDate date = IntegerCodec.icodec.parse(value);

      if (date == null) {
        return null;
      }
      final long edays = date.toEpochDay();

      if (edays >= Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      } else if (edays <= Integer.MIN_VALUE) {
        return Integer.MIN_VALUE;
      }
      return (int)edays;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#format(java.lang.Object)
     */
    @Override
    public String format(Integer value) {
      return IntegerCodec.icodec.format(
        (value != null) ? LocalDate.ofEpochDay(value.longValue()) : null
      );
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#serialize(java.lang.Object, com.datastax.driver.core.ProtocolVersion)
     */
    @Override
    public ByteBuffer serialize(Integer value, ProtocolVersion protocolVersion) {
      return IntegerCodec.icodec.serialize(
        (value != null) ? LocalDate.ofEpochDay(value.longValue()) : null,
        protocolVersion
      );
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#deserialize(java.nio.ByteBuffer, com.datastax.driver.core.ProtocolVersion)
     */
    @Override
    public Integer deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
      final LocalDate date = IntegerCodec.icodec.deserialize(bytes, protocolVersion);

      if (date == null) {
        return null;
      }
      final long edays = date.toEpochDay();

      if (edays >= Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      } else if (edays <= Integer.MIN_VALUE) {
        return Integer.MIN_VALUE;
      }
      return (int)edays;
    }
  }
}
