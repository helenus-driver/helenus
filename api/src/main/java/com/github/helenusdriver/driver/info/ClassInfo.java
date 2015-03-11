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
package com.github.helenusdriver.driver.info;

import com.github.helenusdriver.persistence.Keyspace;

/**
 * The <code>ClassInfo</code> interface provides information about a particular
 * POJO class.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 1.0
 */
public interface ClassInfo<T> {
  /**
   * Gets the class of POJO represented by this class info object.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> type of POJO represented by this class
   *         info
   */
  public Class<T> getObjectClass();

  /**
   * Gets the keyspace annotation for this POJO.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> keyspace annotation for this POJO
   */
  public Keyspace getKeyspace();

  /**
   * Checks of the class of POJO represented by this class supports tables and.or
   * indexes.
   *
   * @author paouelle
   *
   * @return <code>true</code> if the POJO class supports tables and/or indexes
   *         <code>false</code> if it does not
   */
  public boolean supportsTablesAndIndexes();

  /**
   * Gets the field info corresponding to the given suffix key.
   *
   * @author paouelle
   *
   * @param  name the name of the suffix key
   * @return the corresponding field info or <code>null</code> if not defined
   */
  public FieldInfo<T> getSuffixKey(String name);

  /**
   * Gets the field info corresponding to the given suffix type.
   *
   * @author paouelle
   *
   * @param  type the type of the suffix key
   * @return the corresponding field info or <code>null</code> if not defined
   */
  public FieldInfo<T> getSuffixKeyByType(String type);

  /**
   * Gets the table info corresponding to the given table name.
   *
   * @author paouelle
   *
   * @param  name the name of the table to get
   * @return the corresponding table info
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified table is not defined by
   *         the POJO or the POJO represented by this class doesn't support tables
   */
  public TableInfo<T> getTable(String name);

  /**
   * Gets the number of tables defined by the POJO.
   *
   * @author paouelle
   *
   * @return the number of tables defined by the POJO
   */
  public int getNumTables();
}
