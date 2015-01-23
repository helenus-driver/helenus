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

import java.util.Iterator;

import com.github.helenusdriver.persistence.Table;

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
public interface TableInfo<T> {
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
   * @return the non-<code>null</code> table annotation
   */
  public Table getTable();

  /**
   * Gets the table name. The returned table name will be cleaned up
   * according to the Cassandra guidelines. So it might not be the exact same
   * value as was defined in the @Table annotation.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> name for the table
   */
  public String getName();

  /**
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
   */
  public Iterator<FieldInfo<T>> columns();
}
