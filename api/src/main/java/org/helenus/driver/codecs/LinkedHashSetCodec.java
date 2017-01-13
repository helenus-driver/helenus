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

import java.util.LinkedHashSet;

import com.datastax.driver.core.DataType.CollectionType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.TypeCodec.AbstractCollectionCodec;
import com.google.common.reflect.TypeToken;

/**
 * The <code>LinkedHashSetCodec</code> class provides an implementation for a codec
 * capable of handling {@link LinkedHashSet} objects.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 9, 2017 - paouelle - Creation
 *
 * @param <T> the type of element for the set
 *
 * @since 3.0
 */
public class LinkedHashSetCodec<T> extends AbstractCollectionCodec<T, LinkedHashSet<T>> {
  /**
   * Instantiates a new <code>LinkedHashSetCodec</code> object.
   *
   * @author paouelle
   *
   * @param cqlType the CQL type for this codec
   * @param javaType the java type for this codec
   * @param ecodec the element codec
   */
  public LinkedHashSetCodec(
    CollectionType cqlType,
    TypeToken<LinkedHashSet<T>> javaType,
    TypeCodec<T> ecodec
  ) {
    super(cqlType, javaType, ecodec);
  }


  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec.AbstractCollectionCodec#newInstance(int)
   */
  @Override
  protected LinkedHashSet<T> newInstance(int size) {
    return new LinkedHashSet<>(size * 3 / 2);
  }
}
