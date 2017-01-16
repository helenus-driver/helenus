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

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.reflect.TypeToken;

import org.helenus.driver.persistence.DataType;

/**
 * The <code>BlobCodecProvider</code> class defines a provider suitable to
 * decode the {@link DataType#BLOB} data type.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 9, 2017 - paouelle - Creation
 *
 * @since 1.0
 */
public final class BlobCodecProvider implements CodecProvider {
  /**
   * Holds the single instance of this provider.
   *
   * @author paouelle
   */
  public final static CodecProvider INSTANCE = new BlobCodecProvider();

  /**
   * Gets a codec to decode <code>byte[]</code> objects.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> codec to decode <code>byte[]</code> objects
   */
  @SuppressWarnings("synthetic-access")
  public static TypeCodec<byte[]> bytearray() {
    return ByteArrayCodec.instance;
  }

  /**
   * Instantiates a new <code>BlobCodecProvider</code> object.
   *
   * @author paouelle
   */
  private BlobCodecProvider() {}

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
    if (clazz.isArray() && (Byte.TYPE == clazz.getComponentType())) {
      return (TypeCodec<T>)BlobCodecProvider.bytearray();
    } else if (ByteBuffer.class.isAssignableFrom(clazz)) {
      return (TypeCodec<T>)TypeCodec.blob();
    }
    throw new CodecNotFoundException(
      "unsupported Helenus codec from '"
      + DataType.BLOB.toCQL()
      + "' to class: "
      + clazz.getName(),
      com.datastax.driver.core.DataType.blob(),
      TypeToken.of(clazz)
    );
  }

  /**
   * The <code>ByteArrayCodec</code> class provides an implementation for a codec
   * capable of handling <code>byte[]</code> objects.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author The Helenus Driver Project Authors
   * @version 1 - Jan 13, 2017 - paouelle - Creation
   *
   * @since 3.0
   */
  private static class ByteArrayCodec extends TypeCodec<byte[]> {
    /**
     * Holds the instance for this codec.
     *
     * @author paouelle
     */
    private final static TypeCodec<byte[]> instance = new ByteArrayCodec();

    /**
     * Instantiates a new <code>ByteArrayCodec</code> object.
     *
     * @author paouelle
     */
    private ByteArrayCodec() {
      super(com.datastax.driver.core.DataType.blob(), byte[].class);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#parse(java.lang.String)
     */
    @Override
    public byte[] parse(String value) {
      if (StringUtils.isEmpty(value) || value.equalsIgnoreCase("NULL")) {
        return null;
      }
      if ((value.length() & 1) == 1) {
        throw new IllegalArgumentException(
          "a CQL blob string must have an even length (since one byte is always 2 hexadecimal character)"
        );
      }
      if ((value.charAt(0) != '0') || (value.charAt(1) != 'x')) {
        throw new IllegalArgumentException("a CQL blob string must start with \"0x\"");
      }
      return Bytes.fromRawHexString(value, 2);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#format(java.lang.Object)
     */
    @Override
    public String format(byte[] value) {
      if (value == null) {
        return "NULL";
      }
      return Bytes.toHexString(value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#serialize(java.lang.Object, com.datastax.driver.core.ProtocolVersion)
     */
    @Override
    public ByteBuffer serialize(byte[] value, ProtocolVersion protocolVersion) {
      return (value == null) ? null : ByteBuffer.wrap(value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.TypeCodec#deserialize(java.nio.ByteBuffer, com.datastax.driver.core.ProtocolVersion)
     */
    @Override
    public byte[] deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
      if (bytes == null) {
        return null;
      }
      final byte[] bs = new byte[bytes.remaining()];

      bytes.get(bs);
      return bs;
    }
  }
}
