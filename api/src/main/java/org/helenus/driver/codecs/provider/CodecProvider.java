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

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;

/**
 * The <code>CodecProvider</code> interface defines a provider interface to retrieve
 * Helenus-specific codecs.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 9, 2017 - paouelle - Creation
 *
 * @since 3.0
 *
 * @see com.datastax.driver.core.CodecRegistry
 */
public interface CodecProvider {
  /**
   * Gets a {@link TypeCodec codec} that accepts the given class.
   *
   * @author paouelle
   *
   * @param <T> the type of objects to accept
   *
   * @param  clazz the non-<code>null</code> class the codec should accept
   * @return a suitable codec
   * @throws CodecNotFoundException if a suitable codec cannot be found
   */
  public <T> TypeCodec<T> codecFor(Class<T> clazz) throws CodecNotFoundException;
}
