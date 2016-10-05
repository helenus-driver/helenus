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
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.util.Set;

import org.helenus.annotation.Keyable;

/**
 * The <code>PartitionKey</code> annotation indicates the column is a partition
 * key. The partition key determines on which nodes data is stored.
 * <p>
 * It is also possible to specify compound partition keys by annotating
 * multiple fields or properties with the <code>PartitionKey</code> annotation.
 * <p>
 * Special support is given to a partition key defined as {@link Set}. Using
 * such a multi-partition key enables querying for any of its elements to
 * retrieve the object from the database. Underneath the table, in addition to
 * saving the key as a set in the defined column, the driver will also create
 * another column of the element type as the actual partition key and create
 * multiple entries in the table; one per element of the multi-key.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(PartitionKeys.class)
@Keyable("table")
@Documented
public @interface PartitionKey {
  /**
   * The name of the table as defined in {@link Table#name} this partition key is
   * associated with.
   * <p>
   * By default, if none specified, the column will be considered a partition
   * key for all tables defined by the entity unless otherwise overridden.
   *
   * @author paouelle
   *
   * @return the name of the table this partition key is associated with
   */
  String table() default Table.ALL;

  /**
   * Specifies whether or not to ignore case on this key if defined as a string.
   * <p>
   * If set to <code>true</code> a duplicate of the column is created with its
   * value converted to lower case. By default, Helenus will not ignore case.
   *
   * @author paouelle
   *
   * @return <code>true</code> if the annotated string column should be considered
   *         case insensitive; <code>false</code> for normal handling
   */
  boolean ignoreCase() default false;
}
