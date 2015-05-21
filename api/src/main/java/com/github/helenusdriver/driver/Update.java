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
 * The <code>Update</code> interface extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Update} class to provide support
 * for POJOs.
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
public interface Update<T>
  extends ObjectStatement<T>, BatchableStatement<Void, VoidFuture> {
  /**
   * Sets the 'IF EXISTS' option for this UPDATE statement.
   * <p>
   * An update with that option will not succeed unless the row exist at the
   * time the update is executed.
   * <p>
   * Please keep in mind that using this option has a non negligible performance
   * impact and should be avoided when possible and that no other 'if' conditions
   * can be combined.
   *
   * @author paouelle
   *
   * @return this UPDATE statement.
   */
  public Update<T> ifExists();

  /**
   * Adds an assignment to this UPDATE statement.
   *
   * This is a shorter/more readable version for {@code with().and(assignments)}.
   *
   * @author paouelle
   *
   * @param  assignments the assignments to add.
   * @return the Assignments of this UPDATE statement.
   * @throws IllegalArgumentException if the assignment references a column
   *         not defined in the POJO
   */
  public Assignments<T> with(Assignment... assignments);

  /**
   * Returns the assignments of this UPDATE statement.
   *
   * @author paouelle
   *
   * @return the assignments of this UPDATE statement.
   */
  public Assignments<T> with();

  /**
   * Adds a WHERE clause to this statement.
   *
   * This is a shorter/more readable version for {@code where().and(clauses)}.
   *
   * @author paouelle
   *
   * @param  clause the clause to add.
   * @return the where clause of this query to which more clause can be added.
   * @throws IllegalArgumentException if the clause references a column
   *         not defined in the POJO
   */
  public Where<T> where(Clause clause);

  /**
   * Returns a Where statement for this query without adding clause.
   *
   * @author paouelle
   *
   * @return the where clause of this query to which more clause can be added.
   */
  public Where<T> where();

  /**
   * Adds a conditions clause (IF) to this statement.
   * <p>
   * This is a shorter/more readable version for {@code onlyIf().and(condition)}.
   *
   * @param  condition the condition to add.
   * @return the conditions of this query to which more conditions can be added.
   * @throws IllegalArgumentException if the condition reference a column not
   *         defined by the POJO or if the {@link #ifExists} option was first
   *         selected
   */
  public Conditions<T> onlyIf(Clause condition);

  /**
   * Adds a conditions clause (IF) to this statement.
   *
   * @return the conditions of this query to which more conditions can be added.
   * @throws IllegalArgumentException if the {@link #ifExists} option was first
   *         selected
   */
  public Conditions<T> onlyIf();

  /**
   * Adds a new options for this UPDATE statement.
   *
   * @param  using the option to add.
   * @return the options of this UPDATE statement.
   */
  public Options<T> using(Using using);

  /**
   * The <code>Assignments</code> interface defines the assignments of an UPDATE
   * statement.
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
  public interface Assignments<T>
    extends Statement<T>, BatchableStatement<Void, VoidFuture> {
    /**
     * Adds a new assignment for this UPDATE statement.
     *
     * @author paouelle
     *
     * @param  assignments the assignments to add.
     * @return these Assignments.
     * @throws IllegalArgumentException if the assignment references a column
     *         not defined in the POJO
     */
    public Assignments<T> and(Assignment... assignments);

    /**
     * Checks if there are no assignments registered with this UPDATE statement.
     *
     * @author paouelle
     *
     * @return <code>true</code> if there are no assignments registered;
     *         <code>false</code> otherwise
     */
    public boolean isEmpty();

    /**
     * Adds a WHERE clause to the UPDATE statement those assignments are part
     * of.
     *
     * @author paouelle
     *
     * @param  clause the clause to add.
     * @return the WHERE clause of the UPDATE statement those assignments are
     *         part of.
     * @throws IllegalArgumentException if the clause referenced a
     *         column which is not a primary key or an index column in the POJO
     *         or if the clause references columns not defined by the POJO or
     *         invalid values
     */
    public Where<T> where(Clause clause);

    /**
     * Adds an option to the UPDATE statement those assignments are part of.
     *
     * @author paouelle
     *
     * @param  using the using clause to add.
     * @return the options of the UPDATE statement those assignments are part
     *         of.
     */
    public Options<T> using(Using using);

    /**
     * Adds a condition to the UPDATE statement those assignments are part of.
     *
     * @author paouelle
     *
     * @param  condition the condition to add.
     * @return the conditions for the UPDATE statement those assignments are
     *         part of.
     * @throws IllegalArgumentException if the condition reference a column not
     *         defined by the POJO or if the {@link #ifExists} option was first
     *         selected
     */
    public Conditions<T> onlyIf(Clause condition);
  }

  /**
   * The <code>Where</code> interface defines a WHERE clause for an UPDATE statement.
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
    extends Statement<T>, BatchableStatement<Void, VoidFuture> {
    /**
     * Adds the provided clause to this WHERE clause.
     *
     * @author paouelle
     *
     * @param  clause the clause to add.
     * @return this WHERE clause.
     * @throws IllegalArgumentException if the clause references a column
     *         not defined in the POJO
     */
    public Where<T> and(Clause clause);

    /**
     * Adds an assignment to the UPDATE statement this WHERE clause is part of.
     *
     * @author paouelle
     *
     * @param  assignments the assignments to add.
     * @return the assignments of the UPDATE statement this WHERE clause is part
     *         of.
     * @throws IllegalArgumentException if the assignment references a column
     *         not defined in the POJO
     */
    public Assignments<T> with(Assignment... assignments);

    /**
     * Adds an option to the UPDATE statement this WHERE clause is part of.
     *
     * @author paouelle
     *
     * @param  using the using clause to add.
     * @return the options of the UPDATE statement this WHERE clause is part of.
     */
    public Options<T> using(Using using);

    /**
     * Adds a condition to the UPDATE statement this WHERE clause is part of.
     *
     * @author paouelle
     *
     * @param  condition the condition to add.
     * @return the conditions for the UPDATE statement this WHERE clause is part
     *         of.
     * @throws IllegalArgumentException if the condition reference a column not
     *         defined by the POJO or if the {@link #ifExists} option was first
     *         selected
     */
    public Conditions<T> onlyIf(Clause condition);
  }

  /**
   * The <code>Options</code> interface defines the options of an UPDATE statement.
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
  public interface Options<T>
    extends Statement<T>, BatchableStatement<Void, VoidFuture> {
    /**
     * Adds the provided option.
     *
     * @author paouelle
     *
     * @param  using an UPDATE option.
     * @return this {@code Options} object.
     */
    public Options<T> and(Using using);

    /**
     * Adds an assignment to the UPDATE statement those options are part of.
     *
     * @author paouelle
     *
     * @param  assignments the assignments to add.
     * @return the assignments of the UPDATE statement those options are part
     *         of.
     * @throws IllegalArgumentException if the assignment references a column
     *         not defined in the POJO
     */
    public Assignments<T> with(Assignment... assignments);

    /**
     * Adds a condition to the UPDATE statement these options are part of.
     *
     * @author paouelle
     *
     * @param  condition the condition to add.
     * @return the conditions for the UPDATE statement these options are part
     *         of.
     * @throws IllegalArgumentException if the condition reference a column not
     *         defined by the POJO or if the {@link #ifExists} option was first
     *         selected
     */
    public Conditions<T> onlyIf(Clause condition);
  }

  /**
   * Conditions for an UDPATE statement.
   * <p>
   * When provided some conditions, an update will not apply unless the provided
   * conditions applies.
   * <p>
   * Please keep in mind that provided conditions has a non negligible
   * performance impact and should be avoided when possible.
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
  public interface Conditions<T>
    extends Statement<T>, BatchableStatement<Void, VoidFuture> {
    /**
     * Adds the provided condition for the update.
     *
     * @author paouelle
     *
     * @param  condition the condition to add.
     * @return this {@code Conditions} clause.
     * @throws IllegalArgumentException if the condition reference a column not
     *         defined by the POJO
     */
    public Conditions<T> and(Clause condition);

    /**
     * Adds an assignment to the UPDATE statement those conditions are part of.
     *
     * @author paouelle
     *
     * @param  assignments the assignments to add.
     * @return the assignments of the UPDATE statement those conditions are part
     *         of.
     * @throws IllegalArgumentException if the assignment references a column
     *         is not defined in the POJO or invalid values or if missing
     *         mandatory columns are referenced by the new assignment
     */
    public Assignments<T> with(Assignment... assignments);

    /**
     * Adds a where clause to the UPDATE statement these options are part of.
     *
     * @param  clause clause to add.
     * @return the WHERE clause of the UPDATE statement these options are part
     *         of.
     * @throws IllegalArgumentException if the clause referenced a
     *         column which is not a primary key or an index column in the POJO
     *         or if the clause references columns not defined by the POJO or
     *         invalid values
     */
    public Where<T> where(Clause clause);

    /**
     * Adds an option to the UPDATE statement these conditions are part of.
     *
     * @param  using the using clause to add.
     * @return the options of the UPDATE statement these conditions are part of.
     */
    public Options<T> using(Using using);
  }
}
