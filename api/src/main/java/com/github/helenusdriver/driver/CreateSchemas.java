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
package com.github.helenusdriver.driver;

import java.util.Set;

import com.github.helenusdriver.driver.info.ClassInfo;

/**
 * The <code>CreateSchemas</code> interface provides support for a statement
 * which will create all the required elements (keyspace, tables, and indexes)
 * to support the schema for a given package of POJOs. It will take care of
 * creating the required keyspace, tables, and indexes.
 * <p>
 * <i>Note:</i> As opposed to the {@link CreateSchema} statement, this one is
 * designed to create schemas for multiple pojo classes; as such keyspace suffixes
 * are actually registered in the where clause using the suffix type which is
 * meant to organize suffixes across multiple pojo classes and not the suffix
 * name (a.k.a. the column name).
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public interface CreateSchemas
  extends Statement<Void>, SequenceableStatement<Void, VoidFuture> {
  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> This will not be a valid query string since this statement
   * will be creating keyspaces and all defined tables and indexes for all POJO
   * classes in the specified package. It will then be a representation of the
   * query strings for the keyspaces and each table and index creation similar
   * to a "BATCH" statement.
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#getQueryString()
   */
  @Override
  public String getQueryString();

  /**
   * Gets all POJO classes found for which schemas will be created.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> set of all POJO classes found
   */
  public Set<Class<?>> getObjectClasses();

  /**
   * Gets all POJO class informations found for which schemas will be created.
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
   * Sets the 'IF NOT EXISTS' option for this CREATE SCHEMAS statement to be applied
   * to the keyspaces, tables, and indexes creation.
   * <p>
   * A create with that option will not succeed unless the corresponding element
   * does not exist at the time the creation is executing. The existence check
   * and creations are done transactionally in the sense that if multiple clients
   * attempt to create a given keyspace, table, or index with this option, then
   * at most one may succeed.
   * <p>
   * Please keep in mind that using this option has a non negligible performance
   * impact and should be avoided when possible.
   *
   * @author paouelle
   *
   * @return this CREATE SCHEMAS statement.
   */
  public CreateSchemas ifNotExists();

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
   * The <code>Where</code> interface defines a WHERE clause for the CREATE
   * SCHEMAS statement which can be used to specify suffix types used for
   * keyspace names as required by specific POJO class schema creation.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
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
