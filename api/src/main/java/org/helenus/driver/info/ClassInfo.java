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
package org.helenus.driver.info;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.helenus.driver.persistence.Keyspace;

/**
 * The <code>ClassInfo</code> interface provides information about a particular
 * POJO class.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 1.0
 */
public interface ClassInfo<T> extends Iterable<TableInfo<T>> {
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
   * Gets the number of keyspace keys defined by this class of pojo.
   *
   * @author paouelle
   *
   * @return the number of keyspace keys defined by this class of pojo
   */
  public int getNumKeyspaceKeys();

  /**
   * Gets the field info corresponding to the given keyspace key.
   *
   * @author paouelle
   *
   * @param  name the name of the keyspace key
   * @return the corresponding field info or <code>null</code> if not defined
   */
  public FieldInfo<T> getKeyspaceKey(String name);

  /**
   * Gets the field info corresponding to the given keyspace key type.
   *
   * @author paouelle
   *
   * @param  type the type of the keyspace key
   * @return the corresponding field info or <code>null</code> if not defined
   */
  public FieldInfo<T> getKeyspaceKeyByType(String type);

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
   * Gets the primary table if one is defined.
   *
   * @author paouelle
   *
   * @return the primary table defined or empty if none defined
   */
  public Optional<TableInfo<T>> getPrimaryTable();

  /**
   * Gets an audit table if one is defined.
   * <p>
   * <i>Note:</i> The behavior is not guaranteed if more than one audit table
   * are defined for the POJO.
   *
   * @author paouelle
   *
   * @return an audit table defined or empty if none defined
   */
  public Optional<TableInfo<T>> getAuditTable();

  /**
   * Gets the number of tables defined by the POJO.
   *
   * @author paouelle
   *
   * @return the number of tables defined by the POJO
   */
  public int getNumTables();

  /**
   * Gets the tables info defined by the POJO.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> unmodifiable collection of all defined
   *         tables info
   */
  public Collection<TableInfo<T>> getTables();

  /**
   * Gets the tables info defined by the POJO.
   *
   * @author paouelle
   *
   * @return a stream of all defined tables info
   */
  public Stream<TableInfo<T>> tables();

  /**
   * {@inheritDoc}
   *
   * The returned iterator will be unmodifiable.
   *
   * @author paouelle
   *
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<TableInfo<T>> iterator();

  /**
   * Gets the initial objects for the corresponding entity.
   *
   * @author paouelle
   *
   * @param  keys the map of all keyspace key values (if any) keyed by the keyspace
   *         key type
   * @return a non-<code>null</code> collection of the initial objects to insert
   *         in the table
   */
  public Collection<T> getInitialObjects(Map<String, String> keys);
}
