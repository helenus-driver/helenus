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
package org.helenus.driver.persistence;

import java.io.IOException;
import java.io.Serializable;

import org.helenus.commons.lang3.SerializationUtils;

/**
 * The <code>SerializerAndCompressor</code> class provides an implementation for
 * the {@link Persister} interface that uses serialization and compression to
 * generate a blob.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class SerializerAndCompressor implements Persister<Object, byte[]> {
  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.Persister#getDecodedClass()
   */
  @Override
  public Class<Object> getDecodedClass() {
    return Object.class;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.Persister#getPersistedClass()
   */
  @SuppressWarnings("unchecked")
  @Override
  public Class<byte[]> getPersistedClass() {
    return (Class<byte[]>)DataType.BLOB.CLASS;
  }


  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.Persister#encode(java.lang.Object)
   */
  @Override
  public byte[] encode(Object o) throws IOException {
    return SerializationUtils.serializeAndCompress((Serializable)o);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.Persister#decode(Object)
   */
  @Override
  public Object decode(byte[] blob) throws IOException {
    return SerializationUtils.decompressAndDeserialize(blob);
  }
}
