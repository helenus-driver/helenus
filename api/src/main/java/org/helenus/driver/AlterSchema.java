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
package org.helenus.driver;

import java.util.Set;

import org.helenus.driver.info.ClassInfo;

/**
 * The <code>AlterSchema</code> interface provides support for a statement
 * which will create and/or alter all the required elements (i.e. keyspace,
 * tables, types, indexes) to support the schema for a given POJO. It will take
 * care of creating or altering the required keyspace, tables, types, and indexes
 * and will also insert any initial objects defined by the POJO.
 * <p>
 * <i>Note:</i> As opposed to the {@link AlterSchemas} statement, this one is
 * designed to create or alter the specified pojo class schema; as such keyspace
 * suffixes are actually registered in the where clause using the suffix name
 * (a.k.a. the column name) and not the suffix type which is meant to organize
 * suffixes across multiple pojo classes.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Apr 1, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement.
 *
 * @since 1.0
 */
public interface AlterSchema<T>
  extends Statement<T>, SequenceableStatement<Void, VoidFuture> {
  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> This will not be a valid query string since this statement
   * will be creating and/or altering a keyspace and all defined tables, types,
   * and indexes for the POJO class. It will then be a representation of the
   * query strings for the keyspace and each table, type, and index creation
   * similar to a "BATCH" statement.
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getQueryString()
   */
  @Override
  public String getQueryString();

  /**
   * Gets all POJO classes found for the associated POJO for which the schema
   * will be created and/or altered (this might include more than the just the
   * POJO class if this one represents a root entity).
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> set of all POJO classes found
   */
  public Set<Class<?>> getObjectClasses();

  /**
   * Gets all POJO class informations found for the associated POJO for which
   * the schema will be created and/or altered (this might include more than the
   * just the POJO class if this one represents a root entity).
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> set of all POJO class infos found
   */
  public Set<ClassInfo<?>> getClassInfos();

  /**
   * Adds a WHERE clause to this statement used to specify suffixes when required.
   *
   * This is a shorter/more readable version for {@code where().and(clauses)}.
   *
   * @author paouelle
   *
   * @param  clause the clause to add
   * @return the where clause of this query to which more clause can be added.
   * @throws IllegalArgumentException if the clause doesn't reference a
   *         suffix key defined in the POJO
   */
  public Where<T> where(Clause clause);

  /**
   * Returns a WHERE in-construct for this statement without adding clause.
   *
   * @author paouelle
   *
   * @return the where clause of this query to which more clause can be added.
   */
  public Where<T> where();

  /**
   * The <code>Where</code> interface defines a WHERE clause for the ALTER
   * SCHEMA statement which can be used to specify suffix keys used for the
   * keyspace name.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Apr 1, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @since 1.0
   */
  public interface Where<T>
    extends Statement<T>, SequenceableStatement<Void, VoidFuture> {
    /**
     * Adds the provided clause to this WHERE clause.
     *
     * @author paouelle
     *
     * @param  clause the clause to add.
     * @return this WHERE clause.
     * @throws NullPointerException if <code>clause</code> is <code>null</code>
     * @throws IllegalArgumentException if the clause doesn't reference a
     *         suffix key defined in the POJO
     */
    public Where<T> and(Clause clause);
  }
}
