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


/**
 * The <code>CreateTable</code> interface provides support for the
 * CREATE INDEX statement for a POJO.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement.
 *
 * @since 1.0
 */
public interface CreateIndex<T>
  extends Statement<T>, SequenceableStatement<Void, VoidFuture> {
  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> This might not actually be a valid query string if there are
   * more than one index defined in the POJO that needs to be created. It will
   * then be a representation of the query strings for each index creation
   * similar to a "BATCH" statement.
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#getQueryString()
   */
  @Override
  public String getQueryString();

  /**
   * Sets the 'IF NOT EXISTS' option for this CREATE INDEX statement.
   * <p>
   * A create with that option will not succeed unless the index does not exist
   * at the time the creation is executing. The existence check and creations
   * are done transactionally in the sense that if multiple clients attempt to
   * create a given index with this option, then at most one may succeed.
   * <p>
   * Please keep in mind that using this option has a non negligible performance
   * impact and should be avoided when possible.
   *
   * @author paouelle
   *
   * @return this CREATE INDEX statement.
   */
  public CreateIndex<T> ifNotExists();

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
   * The <code>Builder</code> interface defines an in-build construct for the
   * CREATE INDEX statement
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @since 1.0
   */
  public interface Builder<T> {
    /**
     * Specifies a custom class name for the index.
     *
     * @author paouelle
     *
     * @param  customClass the custom class name for the index
     * @return this in-build construct
     */
    public Builder<T> usingClass(String customClass);

    /**
     * Specifies the tables for which to create indexes from the POJO.
     *
     * @author paouelle
     *
     * @param  tables the tables for which to create defined indexes
     * @return the CREATE INDEX statement
     */
    public CreateIndex<T> on(String... tables);

    /**
     * Specifies to create indexes on all defined tables from the POJO.
     *
     * @author paouelle
     *
     * @return the CREATE INDEX statement
     */
    public CreateIndex<T> onAll();
  }

  /**
   * The <code>Where</code> interface defines a WHERE clause for the CREATE
   * INDEX statement which can be used to specify suffix keys used for the
   * keyspace name.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
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
