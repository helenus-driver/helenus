/*
 * Copyright (C) 2015-2015 The Helenus Driver Project Authors.
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
package com.github.helenusdriver.commons.lang3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.SerializationException;

/**
 * The <code>SerializationUtils</code> class defines helper methods for
 * serialization.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class SerializationUtils extends org.apache.commons.lang3.SerializationUtils {
  /**
   * Serializes and compresses a given object to the specified stream.
   * <p>
   * The stream will be closed once the object is written.
   * This avoids the need for a finally clause, and maybe also exception
   * handling, in the application code.
   * <p>
   * The stream passed in is not buffered internally within this method.
   * This is the responsibility of your application if desired.
   *
   * @author paouelle
   *
   * @param  obj the object to serialize and compress
   * @param  outputStream the stream to write to
   * @throws SerializationException (runtime) if the serialization fails
   * @throws NullPointerException if <code>outputStream</code> is <code>null</code>
   */
  public static void serializeAndCompress(Serializable obj, OutputStream outputStream) {
    org.apache.commons.lang3.Validate.notNull(outputStream, "invalid null outputStream");
    try {
      final GZIPOutputStream os = new GZIPOutputStream(outputStream);

      org.apache.commons.lang3.SerializationUtils.serialize(obj, os);
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }

  /**
   * Serializes and compresses a given object.
   *
   * @author paouelle
   *
   * @param  obj the object to serialize and compress
   * @return an array of bytes representing the compressed serialized form of
   *         the object
   * @throws SerializationException (runtime) if the serialization fails
   */
  public static byte[] serializeAndCompress(Serializable obj) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(512);

    SerializationUtils.serializeAndCompress(obj, baos);
    return baos.toByteArray();
  }

  /**
   * Decompresses and deserializes an object from the specified stream.
   * <p>
   * The stream will be closed once the object is written. This
   * avoids the need for a finally clause, and maybe also exception
   * handling, in the application code.
   * <p>
   * The stream passed in is not buffered internally within this method.
   * This is the responsibility of your application if desired.
   *
   * @author paouelle
   *
   * @param  inputStream the serialized object input stream
   * @return the corresponding object
   * @throws NullPointerException if <code>outputStream</code> is <code>null</code>
   * @throws SerializationException (runtime) if the serialization fails
   */
  public static Object decompressAndDeserialize(InputStream inputStream) {
    org.apache.commons.lang3.Validate.notNull(inputStream, "invalid null inputStream");
    try {
      final GZIPInputStream is = new GZIPInputStream(inputStream);

      return org.apache.commons.lang3.SerializationUtils.deserialize(is);
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }

  /**
   * Decompresses and deserializes the blob back into the original object.
   *
   * @author paouelle
   *
   * @param  data the blob to decompress and deserialize
   * @return the corresponding object
   * @throws SerializationException (runtime) if the serialization fails
   */
  public static Object decompressAndDeserialize(byte[] data) {
    return SerializationUtils.decompressAndDeserialize(
      new ByteArrayInputStream(data)
    );
  }
}
