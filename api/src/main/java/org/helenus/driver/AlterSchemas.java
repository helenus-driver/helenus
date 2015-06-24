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
 * The <code>AlterSchemas</code> interface provides support for a statement
 * which will create and/or alter all the required elements (keyspace, tables,
 * and indexes) to support the schema for a given package of POJOs. It will
 * take care of creating the missing required keyspaces, tables, types, and
 * indexes and it will alter existing keyspaces, tables, types, and indexes by
 * first querying the system keyspace to determine the current schema.
 * <p>
 * <i>Note:</i> As opposed to the {@link AlterSchema} statement, this one is
 * designed to create and/or alter schemas for multiple pojo classes; as such
 * keyspace suffixes are actually registered in the where clause using the
 * suffix type which is meant to organize suffixes across multiple pojo classes
 * and not the suffix name (a.k.a. the column name).
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Apr 1, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public interface AlterSchemas
  extends Statement<Void>, SequenceableStatement<Void, VoidFuture> {
  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> This will not be a valid query string since this statement
   * will be creating and/or altering keyspaces and all defined tables, types
   * and indexes for all POJO
   * classes in the specified package. It will then be a representation of the
   * query strings for the keyspaces and each table, type, and index creation
   * similar to a "BATCH" statement.
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getQueryString()
   */
  @Override
  public String getQueryString();

  /**
   * Gets all POJO classes found for which schemas will be created and/or altered.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> set of all POJO classes found
   */
  public Set<Class<?>> getObjectClasses();

  /**
   * Gets all POJO class informations found for which schemas will be created
   * and/or altered.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> set of all POJO class infos found
   */
  public Set<ClassInfo<?>> getClassInfos();

  /**
   * Gets all POJO class informations defined in the corresponding package
   * (whether or not required suffixes are provided in where clause).
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> set of all POJO class infos found
   */
  public Set<ClassInfo<?>> getDefinedClassInfos();

  /**
   * Adds a WHERE clause to this statement used to specify suffixes by types
   * when required for specific POJO class schema creation.
   *
   * This is a shorter/more readable version for {@code where().and(clauses)}.
   *
   * @author paouelle
   *
   * @param  clause the clause to add
   * @return the where clause of this query to which more clause can be added.
   */
  public Where where(Clause clause);

  /**
   * Returns a WHERE in-construct for this statement without adding clause.
   *
   * @author paouelle
   *
   * @return the where clause of this query to which more clause can be added.
   */
  public Where where();

  /**
   * The <code>Where</code> interface defines a WHERE clause for the ALTER
   * SCHEMAS statement which can be used to specify suffix types used for
   * keyspace names as required by specific POJO class schema creation.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Apr 1, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public interface Where
    extends Statement<Void>, SequenceableStatement<Void, VoidFuture> {
    /**
     * Adds the provided clause to this WHERE clause.
     *
     * @author paouelle
     *
     * @param  clause the clause to add.
     * @return this WHERE clause.
     * @throws NullPointerException if <code>clause</code> is <code>null</code>
     */
    public Where and(Clause clause);
  }
}
