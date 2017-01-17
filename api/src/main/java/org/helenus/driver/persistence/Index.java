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
package org.helenus.driver.persistence;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.helenus.annotation.Keyable;

/**
 * The <code>Index</code> annotation provides a way to annotate a {@link Column}
 * annotated field or property such that an index will be created for it.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Indexes.class)
@Keyable("table")
@Documented
public @interface Index {
  /**
   * Constant for SASI indexes.
   *
   * @author paouelle
   */
  public final static String SASI = "org.apache.cassandra.index.sasi.SASIIndex";

  /**
   * Constant for SASI CONTAINS indexes.
   *
   * @author paouelle
   */
  public final static String CONTAINS_MODE = "CONTAINS";

  /**
   * Constant for SASI SPARSE indexes.
   *
   * @author paouelle
   */
  public final static String SPARSE_MODE = "SPARSE";

  /**
   * The name of the table as defined in {@link Table#name} this index is
   * associated with.
   * <p>
   * By default, if none specified, the index will be defined for all tables
   * defined by the entity.
   *
   * @author paouelle
   *
   * @return the name of the table this index is associated with
   */
  String table() default Table.ALL;

  /**
   * Optional index name for the column (must be unique globally in Cassandra).
   * Defaults to Cassandra provided name.
   *
   * @author paouelle
   *
   * @return the optional index name for the column
   */
  String name() default "";

  /**
   * Specifies a custom indexing class.
   *
   * @author paouelle
   *
   * @return the custom indexing class
   */
  String customClass() default "";

  /**
   * Specifies options for the index.
   *
   * @author paouelle
   *
   * @return the options for this index
   */
  String options() default "";
}
