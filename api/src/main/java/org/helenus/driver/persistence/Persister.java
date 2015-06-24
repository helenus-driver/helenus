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

/**
 * The <code>Persister</code> interface provides functionality for persisting
 * any objects by providing both encoding and decoding methods.
 * <p>
 * A persister class must provide either a constructor that accepts an array of
 * strings for configured arguments or a default constructor; if it doesn't
 * support arguments.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> the decoded type resulting from this persister
 * @param <PT> the persisted type from which we are decoding and to which we are
 *             encoding
 *
 * @since 1.0
 */
public interface Persister<T, PT> {
  /**
   * Gets the class this persister decodes to.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> class this persister is decoding to
   */
  public Class<T> getDecodedClass();

  /**
   * Gets the class this persister is persisting to. This should be one of the
   * class from {@link DataType}.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> class this persister is persisting to
   */
  public Class<PT> getPersistedClass();

  /**
   * Encodes the given object into a CQL type ready to be persisted into the
   * database.
   *
   * @author paouelle
   *
   * @param  o the object to encode into an array of bytes (can be <code>null</code>)
   * @return the corresponding encoded object that can later be decoded to recreate
   *         a perfect copy of the object
   * @throws IOException if an error occurs while encoding the object
   */
  public PT encode(T o) throws IOException;

  /**
   * Decodes the given blob into the object that it was originally encoded from.
   *
   * @author paouelle
   *
   * @param  obj the object to decode from (as returned by {@link #encode}
   * @return the corresponding object or <code>null</code> if the encoded object
   *         was <code>null</code> to start with
   * @throws IOException if an error occurs while decoding the object
   */
  public T decode(PT obj) throws IOException;
}
