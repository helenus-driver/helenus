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
package org.helenus.driver;

/**
 * The <code>CreateType</code> interface provides support for the
 * CREATE TYPE statement for a POJO.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Mar 3, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement.
 *
 * @since 1.0
 */
public interface CreateType<T>
  extends Statement<T>, SequenceableStatement<Void, VoidFuture> {
  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getQueryString()
   */
  @Override
  public String getQueryString();

  /**
   * Sets the 'IF NOT EXISTS' option for this CREATE TYPE statement.
   * <p>
   * A create with that option will not succeed unless the type does not exist
   * at the time the creation is executing. The existence check and creations
   * are done transactionally in the sense that if multiple clients attempt to
   * create a given type with this option, then at most one may succeed.
   * <p>
   * Please keep in mind that using this option has a non negligible performance
   * impact and should be avoided when possible.
   *
   * @author paouelle
   *
   * @return this CREATE TYPE statement.
   */
  public CreateType<T> ifNotExists();

  /**
   * Adds a WHERE clause to this statement used to specify keyspace keys when required.
   *
   * This is a shorter/more readable version for {@code where().and(clauses)}.
   *
   * @author paouelle
   *
   * @param  clause the clause to add
   * @return the where clause of this query to which more clause can be added.
   * @throws IllegalArgumentException if the clause doesn't reference a
   *         keyspace key defined in the POJO
   * @throws ExcludedKeyspaceKeyException if the clause reference a keyspace key
   *         and the specified value is marked as excluded
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

//  /**
//   * The <code>Options</code> class defines an in-statement construct for
//   * CREATE TYPE options.
//   *
//   * @copyright 2015-2016 The Helenus Driver Project Authors
//   *
//   * @author  The Helenus Driver Project Authors
//   * @version 1 - Mar 3, 2015 - paouelle - Creation
//   *
//   * @param <T> The type of POJO associated with this statement.
//   *
//   * @since 1.0
//   */
//  public interface Options<T>
//    extends Statement<T>, SequenceableStatement<Void, VoidFuture> {
//    /**
//     * Adds a WHERE clause to this statement used to specify keyspace keys when required.
//     *
//     * This is a shorter/more readable version for {@code where().and(clauses)}.
//     *
//     * @author paouelle
//     *
//     * @param  clause the clause to add
//     * @return the where clause of this query to which more clause can be added.
//     * @throws IllegalArgumentException if the clause doesn't reference a
//     *         keyspace key defined in the POJO
//     * @throws ExcludedKeyspaceKeyException if the clause reference a keyspace key
//     *         and the specified value is marked as excluded
//     */
//    public Where<T> where(Clause clause);
//
//    /**
//     * Returns a WHERE in-construct for this statement without adding clause.
//     *
//     * @author paouelle
//     *
//     * @return the where clause of this query to which more clause can be added.
//     */
//    public Where<T> where();
//  }

  /**
   * The <code>Where</code> interface defines a WHERE clause for the CREATE
   * TYPE statement which can be used to specify keyspace keys used for the
   * keyspace name.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Mar 3, 2015 - paouelle - Creation
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
     *         keyspace key defined in the POJO
     * @throws ExcludedKeyspaceKeyException if the clause reference a keyspace key
     *         and the specified value is marked as excluded
     */
    public Where<T> and(Clause clause);
  }
}
