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


/**
 * The <code>CreateKeyspace</code> interface provides support for the
 * CREATE KEYSPACE statement for a POJO.
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
public interface CreateKeyspace<T>
  extends Statement<T>, SequenceableStatement<Void, VoidFuture> {
  /**
   * Sets the 'IF NOT EXISTS' option for this CREATE KEYSPACE statement.
   * <p>
   * A create with that option will not succeed unless the keyspace does not exist
   * at the time the creation is executing. The existence check and creations
   * are done transactionally in the sense that if multiple clients attempt to
   * create a given keyspace with this option, then at most one may succeed.
   * <p>
   * Please keep in mind that using this option has a non negligible performance
   * impact and should be avoided when possible.
   *
   * @author paouelle
   *
   * @return this CREATE KEYSPACE statement.
   */
  public CreateKeyspace<T> ifNotExists();

  /**
   * Adds a WITH option to this statement.
   * <p>
   * This is a shorter/more readable version for {@code with().and(option)}.
   *
   * @author paouelle
   *
   * @param  option the option to add
   * @return the WITH options of this statement to which more options can be
   *         added
   * @throws NullPointerException if <code>option</code> is <code>null</code>
   */
  public Options<T> with(KeyspaceWith option);

  /**
   * Returns a WITH option for this statement.
   *
   * @author paouelle
   *
   * @return the WITH options of this statement to which more options can be
   *         added
   */
  public Options<T> with();

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
   * @throws ExcludedSuffixKeyException if the clause reference a suffix key
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

  /**
   * The <code>Options</code> class defines an in-statement construct for
   * CREATE KEYSPACE options.
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
  public interface Options<T>
    extends Statement<T>, SequenceableStatement<Void, VoidFuture> {
    /**
     * Adds a WITH option to this statement.
     *
     * @author paouelle
     *
     * @param  option the option to add
     * @return this WITH options
     * @throws NullPointerException if <code>option</code> is <code>null</code>
     */
    public Options<T> and(KeyspaceWith option);

    /**
     * Adds a WHERE clause to this statement used to specify suffixes when required.
     *
     * This is a shorter/more readable version for {@code where().and(clauses)}.
     *
     * @author paouelle
     *
     * @param  clause the clause to add
     * @return the where clause of this query to which more clause can be added
     * @throws IllegalArgumentException if the clause doesn't reference a
     *         suffix key defined in the POJO
     * @throws ExcludedSuffixKeyException if the clause reference a suffix key
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
  }

  /**
   * The <code>Where</code> interface defines a WHERE clause for the CREATE
   * KEYSPACE statement which can be used to specify suffix keys used for the
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
     * @throws ExcludedSuffixKeyException if the clause reference a suffix key
     *         and the specified value is marked as excluded
     */
    public Where<T> and(Clause clause);
  }
}
