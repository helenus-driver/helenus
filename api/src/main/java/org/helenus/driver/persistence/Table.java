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
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.helenus.annotation.Keyable;

/**
 * The <code>Table</code> annotation specifies table information for the annotated
 * object.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Tables.class)
@Keyable("name")
@Inherited
@Documented
public @interface Table {
  /**
   * The <code>Type</code> enumeration is used to define the type for a table.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Dec 17, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public enum Type {
    /**
     * A table resulting in new entries being inserted and existing ones being
     * updated or deleted.
     *
     * @author paouelle
     */
    STANDARD,

    /**
     * A table resulting in new entries always being inserted whether the
     * statement is an INSERT, UPDATE, or DELETE. Note that it is the
     * responsibility of the POJO table definition to properly define the
     * columns such that this will be the case (e.g. adding a version
     * column to the primary key); Helenus will simply generate INSERT
     * statements every time.
     * <p>
     * <i>Note:</i> This behavior only kicks in if the statement is simply
     * created with a reference to the POJO and not by listing specific columns.
     *
     * @author paouelle
     */
    AUDIT,

    /**
     * A table resulting in new entries being inserted and existing ones being
     * updated (even with a delete statement). Typically the table definition
     * should include a column to indicate that the object was deleted and set
     * that flag to <code>true</code> prior to executing a DELETE statement.
     * <p>
     * <i>Note:</i> This behavior only kicks in if the statement is simply
     * created with a reference to the POJO and not by listing specific columns.
     *
     * @author paouelle
     */
    NO_DELETE
  };

  /**
   * Special constants used in the {@link Column}, {@link PartitionKey}, and
   * {@link ClusteringKey} annotations to indicate the annotation applies to
   * all tables defined for the entity.
   *
   * @author paouelle
   */
  public static final String ALL = "@all";

  /**
   * The name for the table. Valid table names are strings of alpha-numeric
   * characters and underscores, and must begin with a letter.
   *
   * @author paouelle
   *
   * @return the name for the table
   */
  String name();

  /**
   * Flag indicating if this table is the primary table for the POJO. There can
   * only be one primary table for a POJO and it is used to compute a primary
   * key when needed.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this is the primary table for the POJO;
   *         <code>false</code> otherwise (default)
   */
  boolean primary() default false;

  /**
   * The type of table this is.
   *
   * @author paouelle
   *
   * @return the type for this table (defaults to a standard table)
   */
  Type type() default Type.STANDARD;

  /**
   * Optional list of ordered partition keys. By default, partition keys are
   * ordered based on the order they are defined in the class. However, the
   * order can be tweaked by specifying a list of column names. Any keys not
   * listed will be added at the end of the provided list.
   *
   * @author paouelle
   *
   * @return the optional list of ordered partition keys
   */
  String[] partition() default {};

  /**
   * Optional list of ordered clustering keys. By default, clustering keys are
   * ordered based on the order they are defined in the class. However, the
   * order can be tweaked by specifying a list of column names. Any keys not
   * listed will be added at the end of the provided list.
   *
   * @author paouelle
   *
   * @return the optional list of ordered clustering keys
   */
  String[] clustering() default {};

  /**
   * The optional compaction strategy class for the table. Defaults to
   * {@link CompactionClass#SIZED_TIERED}.
   *
   * @author paouelle
   *
   * @return the optional compaction strategy class
   */
  CompactionClass compaction() default CompactionClass.SIZED_TIERED;

  /**
   * The optional compaction options defined as a Json object with {}.
   *
   * @author paouelle
   *
   * @return the optional compaction options
   */
  String compactionOptions() default "";
}
