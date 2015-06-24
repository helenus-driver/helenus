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
package org.helenus.driver.info;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import org.helenus.driver.persistence.Table;

/**
 * The <code>TableInfo</code> class provides information about a specific table
 * for a particular POJO class.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 2.0
 */
public interface TableInfo<T> extends Iterable<FieldInfo<T>> {
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
   * Gets the class info for the POJO.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> class info for the POJO
   */
  public ClassInfo<T> getClassInfo();

  /**
   * Gets the table annotation for this info object.
   *
   * @author paouelle
   *
   * @return the table annotation (will be <code>null</code> for user-defined
   *         type entities)
   */
  public Table getTable();

  /**
   * Gets the table name. The returned table name will be cleaned up
   * according to the Cassandra guidelines. So it might not be the exact same
   * value as was defined in the @Table annotation.
   *
   * @author paouelle
   *
   * @return the name for the table (will be <code>null</code> for user-defined
   *         type entities)
   */
  public String getName();

  /**
   * Checks if this table defines columns as collections.
   *
   * @author paouelle
   *
   * @return <code>true</code> if at least one column for this table is defined
   *         as a collection; <code>false</code> otherwise
   */
  public boolean hasCollectionColumns();

  /**
   * {@inheritDoc}
   *
   * Gets an iterator of all fields defined as columns in this table.
   * <p>
   * <i>Note:</i> The {@link Iterator#remove} method is fully supported and if
   * called, will remove this field from the persisted state of the entity. This
   * can be useful for filters.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> iterator of all fields defined as columns
   *         in this table
   *
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<FieldInfo<T>> iterator();

  /**
   * Gets the set of column fields for the POJO in this table.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> unmodifiable collection of all column fields
   */
  public Collection<FieldInfo<T>> getColumns();

  /**
   * Gets the set of column fields for the POJO in this table.
   *
   * @author paouelle
   *
   * @return a stream of all column fields
   */
  public Stream<FieldInfo<T>> columns();
}
