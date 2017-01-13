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

import java.util.Date;

import java.nio.ByteBuffer;
import java.time.Instant;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.google.common.reflect.TypeToken;

import org.helenus.driver.persistence.DataType;

/**
 * The <code>TimestampCodecProvider</code> class defines a provider suitable to
 * decode the {@link DataType#TIMESTAMP} data type.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 9, 2017 - paouelle - Creation
 *
 * @since 3.0
 */
public final class TimestampCodecProvider implements CodecProvider {
  /**
   * Holds the single instance of this provider.
   *
   * @author paouelle
   */
  public final static CodecProvider INSTANCE = new TimestampCodecProvider();

  /**
   * Gets a codec to decode {@link Long} objects.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> codec to decode {@link Long} objects
   */
  @SuppressWarnings("synthetic-access")
  public static TypeCodec<Long> clong() {
    return LongCodec.instance;
  }

  /**
   * Gets a codec to decode {@link Instant} objects.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> codec to decode {@link Instant} objects
   */
  @SuppressWarnings("synthetic-access")
  public static TypeCodec<Instant> instant() {
    return InstantCodec.instance;
  }

  /**
   * Instantiates a new <code>TimestampCodecProvider</code> object.
   *
   * @author paouelle
   */
  private TimestampCodecProvider() {}

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
      return (TypeCodec<T>)TimestampCodecProvider.clong();
    } else if (Date.class.isAssignableFrom(clazz)) {
      return (TypeCodec<T>)TypeCodec.timestamp();
    } else if (Instant.class.isAssignableFrom(clazz)) {
      return (TypeCodec<T>)TimestampCodecProvider.instant();
    }
    throw new CodecNotFoundException(
      "unsupported Helenus codec from '"
      + DataType.TIMESTAMP.toCQL()
      + "' to class: "
      + clazz.getName(),
      com.datastax.driver.core.DataType.timestamp(),
      TypeToken.of(clazz)
    );
  }

  /**
   * The <code>LongCodec</code> class defines a type codec for {@link Long}
   * objects.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author The Helenus Driver Project Authors
   * @version 1 - Jan 9, 2017 - paouelle - Creation
   *
   * @since 3.0
   */
  private static class LongCodec extends TypeCodec<Long> {
    /**
     * Holds the instance for this codec.
     *
     * @author paouelle
     */
    private static LongCodec instance = new LongCodec();

    /**
     * Holds the internal codec used to decode.
     *
     * @author paouelle
     */
    private static TypeCodec<Date> icodec = TypeCodec.timestamp();

    /**
     * Instantiates a new <code>LongCodec</code> object.
     *
     * @author paouelle
     */
    private LongCodec() {
      super(com.datastax.driver.core.DataType.timestamp(), Long.class);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#parse(java.lang.String)
     */
    @Override
    public Long parse(String value) {
      final Date date = LongCodec.icodec.parse(value);

      return (date != null) ? date.getTime() : null;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#format(java.lang.Object)
     */
    @Override
    public String format(Long value) {
      return LongCodec.icodec.format((value != null) ? new Date(value) : null);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#serialize(java.lang.Object, com.datastax.driver.core.ProtocolVersion)
     */
    @Override
    public ByteBuffer serialize(Long value, ProtocolVersion protocolVersion) {
      return LongCodec.icodec.serialize((value != null) ? new Date(value) : null, protocolVersion);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#deserialize(java.nio.ByteBuffer, com.datastax.driver.core.ProtocolVersion)
     */
    @Override
    public Long deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
      final Date date = LongCodec.icodec.deserialize(bytes, protocolVersion);

      return (date != null) ? date.getTime() : null;
    }
  }

  /**
   * The <code>InstantCodec</code> class defines a type codec for {@link Instant}
   * objects.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author The Helenus Driver Project Authors
   * @version 1 - Jan 9, 2017 - paouelle - Creation
   *
   * @since 3.0
   */
  private static class InstantCodec extends TypeCodec<Instant> {
    /**
     * Holds the instance for this codec.
     *
     * @author paouelle
     */
    private static InstantCodec instance = new InstantCodec();

    /**
     * Holds the internal codec used to decode.
     *
     * @author paouelle
     */
    private static TypeCodec<Date> icodec = TypeCodec.timestamp();

    /**
     * Instantiates a new <code>InstantCodec</code> object.
     *
     * @author paouelle
     */
    private InstantCodec() {
      super(com.datastax.driver.core.DataType.timestamp(), Instant.class);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#parse(java.lang.String)
     */
    @Override
    public Instant parse(String value) {
      final Date date = InstantCodec.icodec.parse(value);

      return (date != null) ? date.toInstant() : null;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#format(java.lang.Object)
     */
    @Override
    public String format(Instant value) {
      return InstantCodec.icodec.format((value != null) ? Date.from(value) : null);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#serialize(java.lang.Object, com.datastax.driver.core.ProtocolVersion)
     */
    @Override
    public ByteBuffer serialize(Instant value, ProtocolVersion protocolVersion) {
      return InstantCodec.icodec.serialize((value != null) ? Date.from(value) : null, protocolVersion);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#deserialize(java.nio.ByteBuffer, com.datastax.driver.core.ProtocolVersion)
     */
    @Override
    public Instant deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
      final Date date = InstantCodec.icodec.deserialize(bytes, protocolVersion);

      return (date != null) ? date.toInstant() : null;
    }
  }
}
