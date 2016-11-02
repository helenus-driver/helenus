/*
 * Copyright (C) 2015-2016 The Helenus Driver Project Authors.
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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.net.InetAddress;

import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;

import org.helenus.annotation.Keyable;

/**
 * The <code>Column</code> annotation specifies a mapped column for a persistent
 * property or field.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Columns.class)
@Keyable("table")
@Documented
public @interface Column {
  /**
   * The name of the table as defined in {@link Table#name} this column is
   * associated with.
   * <p>
   * By default, if none specified, the column will be persisted in all tables
   * defined by the entity.
   *
   * @author paouelle
   *
   * @return the name of the table this column is associated with
   */
  String table() default Table.ALL;

  /**
   * The name of the column.
   *
   * @author paouelle
   *
   * @return the name of the column
   */
  String name();

  /**
   * Flag indicating if the column should be a static column. A static column is
   * common to all clustered rows of a given partition.
   * <p>
   * <i>Note:</i> Only valid for non-partition, non-clustering, and non-keyspace
   * key columns of tables. Not supported for user-defined types.
   *
   * @author paouelle
   *
   * @return a flag indicating if the column should be a static column
   */
  boolean isStatic() default false;

  /**
   * The <code>Data</code> annotation is used to override the default mapping of
   * data types from the field's class of a column with a specified one. This
   * annotation applies to all tables the column might be persisted to.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  public @interface Data {
    /**
     * The optional CQL data type. If not provided, the data type is inferred
     * from the corresponding attribute type:
     * - "ascii"     - {@link Enum}, {@link Class}, {@link Locale}, or {@link ZoneId}
     * - "bigint"    - {@link Long}
     * - "blob"      - <code>byte[]</code> or {@link ByteBuffer}
     * - "boolean"   - {@link Boolean}
     * - "counter"   - {@link AtomicLong}
     * - "decimal"   - {@link BigDecimal}
     * - "double"    - {@link Double}
     * - "float"     - {@link Float}
     * - "inet"      - {@link InetAddress}
     * - "int"       - {@link Integer}
     * - "text"      - {@link String}
     * - "timestamp" - {@link Date} or {@link Instant}
     * - "uuid"      - {@link UUID}
     * - "varint"    - {@link BigInteger}
     *
     * - "list&lt;ctype&gt;"       - {@link List} or {@link LinkedHashSet} of the corresponding element type
     * - "map&lt;ctype, ctype&gt;" - {@link Map} of the corresponding element types
     * - "set&lt;ctype&gt;"        - {@link Set} of the corresponding element type
     *
     * @author paouelle
     *
     * @return the optional CQL data type
     */
    DataType type() default DataType.INFERRED;

    /**
     * Optionally indicates the argument types for a collection type. Only used
     * when {@link DataType#LIST}, {@link DataType#SET}, {@link DataType#ORDERED_SET},
     * {@link DataType#SORTED_SET}, {@link DataType#MAP}, or
     * {@link DataType#SORTED_MAP} is defined as {@link #type} and cannot be set
     * to one of the collection type either.
     *
     * @author paouelle
     *
     * @return the optional argument types for a collection type or empty to
     *         infer them automatically
     */
    DataType[] arguments() default {};
  }
}
