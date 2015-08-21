/*
 * Copyright (C) 2015-2015 The Helenus Driver Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may",t use this file except in compliance with the License.
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
package org.helenus.driver;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The <code>Keywords</code> class provides access to Cassandra keyword
 * related functionnality.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Aug 21, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public abstract class Keywords {
  /**
   * Non-reserved keywords.
   *
   * @author paouelle
   */
  private static Set<String> NON_RESERVED = new HashSet<>(Arrays.asList(
    "ALL",
    "AS",
    "ASCII",
    "BIGINT",
    "BLOB",
    "BOOLEAN",
    "CLUSTERING",
    "COMPACT",
    "CONSISTENCY",
    "COUNT",
    "COUNTER",
    "CUSTOM",
    "DECIMAL",
    "DISTINCT",
    "DOUBLE",
    "EXISTS",
    "FILTERING",
    "FLOAT",
    "FROZEN",
    "INT",
    "KEY",
    "LEVEL",
    "LIST",
    "MAP",
    "NOSUPERUSER",
    "PERMISSION",
    "PERMISSIONS",
    "STATIC",
    "STORAGE",
    "SUPERUSER",
    "TEXT",
    "TIMESTAMP",
    "TIMEUUID",
    "TTL",
    "TUPLE",
    "TYPE",
    "USER",
    "USERS",
    "UUID",
    "VALUES",
    "VARCHAR",
    "VARINT",
    "WRITETIME"
  ));

  /**
   * Reserved keywords.
   *
   * @author paouelle
   */
  private static Set<String> RESERVED = new HashSet<>(Arrays.asList(
    "ADD",
    "ALLOW",
    "ALTER",
    "AND",
    "ANY",
    "APPLY",
    "ASC",
    "AUTHORIZE",
    "BATCH",
    "BEGIN",
    "BY",
    "COLUMNFAMILY",
    "CREATE",
    "DELETE",
    "DESC",
    "DROP",
    "EACH_QUORUM",
    "FROM",
    "GRANT",
    "IF",
    "IN",
    "INDEX",
    "INET",
    "INFINITY",
    "INSERT",
    "INTO",
    "KEYSPACE",
    "KEYSPACES",
    "LIMIT",
    "LOCAL_ONE",
    "LOCAL_QUORUM",
    "MODIFY",
    "NAN",
    "NORECURSIVE",
    "NOT",
    "NULL",
    "OF",
    "ON",
    "ONE",
    "ORDER",
    "PASSWORD",
    "PRIMARY",
    "QUORUM",
    "RENAME",
    "REVOKE",
    "SCHEMA",
    "SELECT",
    "SET",
    "TABLE",
    "THREE",
    "TO",
    "TOKEN",
    "TRUNCATE",
    "TWO",
    "UNLOGGED",
    "UPDATE",
    "USE",
    "USING",
    "WHERE",
    "WITH"
  ));

  /**
   * Checks if the specified string is a reserved Cassandra keyword. Reserved
   * keyword cannot be used as an identifier unless you enclose the word in
   * double quotation marks
   * <p>
   * <i>Note:</i> The check is case insensitive.
   *
   * @author paouelle
   *
   * @param  value the value to check for
   * @return <code>true</code> if the value is a reserved keyword; <code>false</code>
   *         otherwise
   */
  public static boolean isReserved(String value) {
    if (value == null) {
      return false;
    }
    return Keywords.RESERVED.contains(value.toUpperCase());
  }

  /**
   * Checks if the specified string is a non-reserved Cassandra keyword.
   * Non-reserved keywords have a specific meaning in certain context but can
   * be used as an identifier outside this context
   * <p>
   * <i>Note:</i> The check is case insensitive.
   *
   * @author paouelle
   *
   * @param  value the value to check for
   * @return <code>true</code> if the value is a non-reserved keyword; <code>false</code>
   *         otherwise
   */
  public static boolean isNonReserved(String value) {
    if (value == null) {
      return false;
    }
    return Keywords.NON_RESERVED.contains(value.toUpperCase());
  }

  /**
   * Prevents instantiation.
   *
   * @author paouelle
   */
  private Keywords() {}
}
