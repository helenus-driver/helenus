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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The <code>Persisted</code> annotation can be used on a {@link Column}
 * annotated field or on a {@link UDTEntity} annotated user-defined class that
 * implements {@link List}, {@link Set}, or {@link Map} in order to indicate the
 * field or the collection elements should be encoded before being persisted to
 * the database using a specified persister. The data type for the column or the
 * collected elements will automatically be set using the data type specified
 * as part of the annotation. When persisting a collection column or class, each
 * elements of the collection will be be passed into the persister before being
 * saved to the database. The resulting database data type for the column will
 * be changed based on the data type specified here. For example, if the data
 * type is set to "blob" the data type for collection columns or classes would
 * be "list&lt;blob&gt;", "map&lt;?,blob&gt;", or "set&lt;blob&gt;".
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Persisted {
  /**
   * The persister class to use for the column or the elements of the
   * collection column.
   *
   * @author paouelle
   *
   * @return the persister class
   */
  Class<? extends Persister<?, ?>> using();

  /**
   * The resulting CQL data type after persistence. For collections, this would
   * be the element's or value's data type. For maps, the data type for the keys
   * is preserved from the column's or class' definition.
   * <p>
   * <i>Note:</i> that the data type specified cannot be one of
   * {@link DataType#INFERRED}, {@link DataType#LIST}, {@link DataType#SET},
   * {@link DataType#ORDERED_SET} {@link DataType#MAP}, or
   * {@link DataType#SORTED_MAP}. Otherwise an error will be thrown when
   * analyzing the POJO at runtime.
   *
   * @author paouelle
   *
   * @return the resulting CQL data type after persistence
   */
  DataType as();

  /**
   * Persister specific arguments.
   *
   * @author paouelle
   *
   * @return the persister specific arguments
   */
  String[] arguments() default {};
}
